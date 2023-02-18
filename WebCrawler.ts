import Crawler, { CrawlerRequestResponse } from "crawler";
import mysql, { OkPacket } from "mysql2/promise";
import winston from "winston";

export interface WebCrawlerOptions {
  maxConnections?: number;
  rateLimit?: number;
  userAgent?: string;
  tableName?: string;
  urlColumnName?: string;
  titleColumnName?: string;
  descriptionColumnName?: string;
  rawHtmlColumnName?: string;
  titleSelector?: string;
  descriptionSelector?: string;
  maxDepth?: number;
  queueTableName?: string;
  isCrawledColumnName?: string;
}

export default class WebCrawler {
  private readonly pool: mysql.Pool;
  private readonly crawler: Crawler;
  private readonly targetUrl: string;
  private readonly maxDepth: number;
  private readonly tableName: string;
  private readonly urlColumnName: string;
  private readonly titleColumnName: string;
  private readonly descriptionColumnName: string;
  private readonly rawHtmlColumnName: string;
  private readonly titleSelector: string;
  private readonly descriptionSelector: string;
  private readonly queueTableName: string;
  private readonly isCrawledColumnName: string;

  private readonly logger: winston.Logger;

  constructor(
    host: string,
    user: string,
    password: string,
    database: string,
    targetUrl: string,
    options: WebCrawlerOptions = {}
  ) {
    this.pool = mysql.createPool({
      host,
      user,
      password,
      database,
      ssl: { rejectUnauthorized: false }, // use SSL
    });
    this.targetUrl = targetUrl;
    this.maxDepth = options.maxDepth || 3;
    this.tableName = options.tableName || "pages";
    this.urlColumnName = options.urlColumnName || "url";
    this.titleColumnName = options.titleColumnName || "title";
    this.descriptionColumnName = options.descriptionColumnName || "description";
    this.rawHtmlColumnName = options.rawHtmlColumnName || "raw_html";
    this.titleSelector = options.titleSelector || "title";
    this.descriptionSelector =
      options.descriptionSelector || 'meta[name="description"]';
    this.queueTableName = options.queueTableName || "queue";
    this.isCrawledColumnName = options.isCrawledColumnName || "is_crawled";

    this.crawler = new Crawler({
      maxConnections: options.maxConnections || 10,
      rateLimit: options.rateLimit || 1000,
      userAgent: options.userAgent || "WebCrawler/1.0",
      callback: this.processPage.bind(this),
    });

    this.logger = winston.createLogger({
      level: "info",
      format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.splat(),
        winston.format.json()
      ),
      transports: [
        new winston.transports.Console({
          format: winston.format.combine(
            winston.format.colorize(),
            winston.format.simple()
          ),
        }),
      ],
    });
  }
  private async dequeueUrls(count: number): Promise<string[]> {
    const urls: string[] = [];
    let dequeuedCount = 0;

    while (dequeuedCount < count) {
      const url = await this.dequeueUrl();

      if (url === undefined) {
        break;
      }

      urls.push(url);
      dequeuedCount++;
    }

    return urls;
  }
  private async dequeueUrl(): Promise<string | undefined> {
    const connection = await this.pool.getConnection();

    try {
      const [rows] = (await connection.query(
        `
      SELECT id FROM \`${this.queueTableName}\` WHERE \`${this.isCrawledColumnName}\` = 0 AND \`depth\` <= ?
      ORDER BY \`depth\` ASC, \`id\` ASC LIMIT ?
    `,
        [this.maxDepth, 1]
      )) as any;

      const ids = rows.map((row: any) => (row as any).id);
      console.error(rows);

      if (ids.length === 0) {
        return undefined;
      }

      await connection.query(
        `
      UPDATE \`${this.queueTableName}\` SET \`${this.isCrawledColumnName}\` = 1 WHERE \`id\` IN (?)
    `,
        [ids]
      );

      return (rows[0] as any)[this.urlColumnName];
    } catch (error) {
      if (error instanceof Error)
        this.logger.error(`Error dequeuing URL: ${error.message}`);
      return undefined;
    } finally {
      connection.release();
    }
  }
  public async start() {
    this.logger.info("Starting WebXenon...");

    try {
      await this.createTables();
      let url = await this.dequeueUrl();

      while (url !== undefined) {
        const crawler = new Crawler({
          maxDepth: this.maxDepth,
          callback: this.processPage.bind(this),
        });
        crawler.queue(url);

        url = await this.dequeueUrl();
      }
    } catch (error) {
      if (error instanceof Error)
        this.logger.error(`Error starting WebXenon: ${error.message}`);
    }

    this.logger.info("WebXenon stopped.");
  }
  private async processPage(
    error: Error | null,
    res: CrawlerRequestResponse,
    done: () => void
  ) {
    if (error) {
      this.logger.error("Error crawling URL:", res.options.uri, error);
      done();
      return;
    }

    const url = res.options.uri;
    const depth = res.options.depth;
    const $ = res.$;

    try {
      // Extract page title and description
      const title = $(this.titleSelector).text().trim();
      const description = $(this.descriptionSelector).attr("content")?.trim();

      // Insert or update page in database
      const connection = await this.pool.getConnection();

      try {
        const [result] = await connection.query(
          `
        SELECT * FROM \`${this.tableName}\` WHERE \`${this.urlColumnName}\` = ?
      `,
          [url]
        );

        if (Array.isArray(result) && result.length > 0) {
          // Page already exists, update it
          await connection.query(
            `
          UPDATE \`${this.tableName}\` SET
            \`${this.titleColumnName}\` = ?,
            \`${this.descriptionColumnName}\` = ?,
            \`${this.rawHtmlColumnName}\` = ?
          WHERE \`${this.urlColumnName}\` = ?
        `,
            [title, description, $.html(), url]
          );
        } else {
          // Page doesn't exist, insert it
          await connection.query(
            `
          INSERT INTO \`${this.tableName}\`
            (\`${this.urlColumnName}\`, \`${this.titleColumnName}\`, \`${this.descriptionColumnName}\`, \`${this.rawHtmlColumnName}\`)
          VALUES (?, ?, ?, ?)
        `,
            [url, title, description, $.html()]
          );
        }

        // Mark URL as crawled
        await connection.query(
          `
        UPDATE \`${this.queueTableName}\` SET \`${this.isCrawledColumnName}\` = 1 WHERE \`${this.urlColumnName}\` = ?
      `,
          [url]
        );

        connection.release();
      } catch (error) {
        connection.release();
        throw error;
      }

      if (depth < this.maxDepth) {
        // Extract links and add them to the queue
        const links = $("a")
          .map((i, el) => $(el).attr("href"))
          .get()
          .filter((link) => link && link.startsWith(this.targetUrl));

        this.queueUrls(links);
      }

      this.logger.info("Crawled URL:", url, { depth });
    } catch (error) {
      this.logger.error("Error processing URL:", url, error);
    } finally {
      done();
    }
  }

  private async createTables() {
    const connection = await this.pool.getConnection();

    try {
      await connection.query(`
      CREATE TABLE IF NOT EXISTS \`${this.tableName}\` (
        \`id\` INT(11) NOT NULL AUTO_INCREMENT,
        \`${this.urlColumnName}\` TEXT NOT NULL,
        \`${this.titleColumnName}\` TEXT DEFAULT NULL,
        \`${this.descriptionColumnName}\` TEXT DEFAULT NULL,
        \`${this.rawHtmlColumnName}\` LONGTEXT DEFAULT NULL,
        PRIMARY KEY (\`id\`),
        UNIQUE KEY \`${this.urlColumnName}\` (\`${this.urlColumnName}\`(255))
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);

      await connection.query(`
      CREATE TABLE IF NOT EXISTS \`${this.queueTableName}\` (
        \`id\` INT(11) NOT NULL AUTO_INCREMENT,
        \`${this.urlColumnName}\` TEXT NOT NULL,
        \`depth\` INT(11) NOT NULL DEFAULT 0,
        \`${this.isCrawledColumnName}\` TINYINT(1) NOT NULL DEFAULT 0,
        PRIMARY KEY (\`id\`),
        UNIQUE KEY \`${this.urlColumnName}\` (\`${this.urlColumnName}\`(255))
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);

      await connection.query(
        `
      INSERT INTO \`${this.queueTableName}\` (\`${this.urlColumnName}\`, \`depth\`) VALUES (?, ?)
    `,
        [this.targetUrl, 0]
      );
    } catch (error) {
      // check error type is Error
      if (error instanceof Error) {
        this.logger.error(`Error creating tables: ${error.message}`);
      }
    } finally {
      connection.release();
    }
  }

  private async queueUrls(urls: string[]): Promise<void> {
    const connection = await this.pool.getConnection();

    try {
      await connection.beginTransaction();

      for (const url of urls) {
        try {
          const [result] = (await connection.query(
            `
          INSERT INTO \`${this.queueTableName}\`
            (\`${this.urlColumnName}\`, \`depth\`)
          VALUES (?, ?)
        `,
            [url, 0]
          )) as mysql.OkPacket[];

          if (result?.insertId) {
            this.logger.info(`Queued URL: ${url}`);
          }
        } catch (error: any) {
          if (error.code !== "ER_DUP_ENTRY") {
            throw error;
          }
        }
      }

      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  async stop() {
    this.crawler.on("drain", () => {
      this.logger.info("Crawler has stopped.");
      this.pool.end();
    });

    this.crawler.removeAllListeners();
    this.logger.info("Crawler is stopping...");
  }
}
