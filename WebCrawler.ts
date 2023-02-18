import Crawler, { CrawlerRequestResponse } from "crawler";
import mysql from "mysql2/promise";
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

  async start() {
    await this.createTables();

    this.queueUrls([this.targetUrl]);
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
        await connection.beginTransaction();

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

        await connection.commit();
      } catch (error) {
        await connection.rollback();
        throw error;
      } finally {
        connection.release();
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
          UNIQUE KEY \`${this.urlColumnName}\` (\`${this.urlColumnName}\`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `);

      await connection.query(`
        CREATE TABLE IF NOT EXISTS \`${this.queueTableName}\` (
          \`id\` INT(11) NOT NULL AUTO_INCREMENT,
          \`${this.urlColumnName}\` TEXT NOT NULL,
          \`${this.isCrawledColumnName}\` TINYINT(1) NOT NULL DEFAULT '0',
          PRIMARY KEY (\`id\`),
          UNIQUE KEY \`${this.urlColumnName}\` (\`${this.urlColumnName}\`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `);
    } catch (error) {
      this.logger.error("Error creating database tables:", error);
      throw error;
    } finally {
      connection.release();
    }
  }

  private async queueUrls(urls: string[]) {
    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();

      for (const url of urls) {
        const [result] = await connection.query(
          `
          SELECT * FROM \`${this.queueTableName}\` WHERE \`${this.urlColumnName}\` = ?
        `,
          [url]
        );

        if (Array.isArray(result) && result.length === 0) {
          await connection.query(
            `
            INSERT INTO \`${this.queueTableName}\` (\`${this.urlColumnName}\`) VALUES (?)
          `,
            [url]
          );
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
