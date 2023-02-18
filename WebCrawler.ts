import Crawler from "crawler";
import mysql from "mysql2/promise";
import { promisify } from "util";
import { spawn } from "child_process";

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

class WebCrawler {
  private readonly pool: mysql.Pool;
  private readonly crawler: Crawler;
  private readonly targetUrl: string;
  private readonly tableName: string;
  private readonly urlColumnName: string;
  private readonly titleColumnName: string;
  private readonly descriptionColumnName: string;
  private readonly rawHtmlColumnName: string;
  private readonly titleSelector: string;
  private readonly descriptionSelector: string;
  private readonly maxDepth: number;
  private readonly queueTableName: string;
  private readonly isCrawledColumnName: string;

  constructor(
    host: string,
    user: string,
    password: string,
    database: string,
    targetUrl: string,
    options: WebCrawlerOptions = {}
  ) {
    const {
      maxConnections = 10,
      rateLimit = 1000,
      userAgent = "My Web Crawler",
      tableName = "pages",
      urlColumnName = "url",
      titleColumnName = "title",
      descriptionColumnName = "description",
      rawHtmlColumnName = "raw_html",
      titleSelector = "title",
      descriptionSelector = 'meta[name="description"]',
      maxDepth = 0,
      queueTableName = "queue",
      isCrawledColumnName = "is_crawled",
    } = options;

    this.pool = mysql.createPool({
      host,
      user,
      password,
      database,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
      ssl: { rejectUnauthorized: false }, // use SSL
    });

    this.crawler = new Crawler({
      maxConnections,
      rateLimit,
      userAgent,
      callback: this.crawl.bind(this),
      depthLimit: maxDepth,
    });

    this.targetUrl = targetUrl;
    this.tableName = tableName;
    this.urlColumnName = urlColumnName;
    this.titleColumnName = titleColumnName;
    this.descriptionColumnName = descriptionColumnName;
    this.rawHtmlColumnName = rawHtmlColumnName;
    this.titleSelector = titleSelector;
    this.descriptionSelector = descriptionSelector;
    this.maxDepth = maxDepth;
    this.queueTableName = queueTableName;
    this.isCrawledColumnName = isCrawledColumnName;
  }

  public async start() {
    const createPagesTable = async () => {
      const conn = await this.pool.getConnection();
      try {
        await conn.query(`
          CREATE TABLE IF NOT EXISTS ${this.tableName} (
            id INT(11) NOT NULL AUTO_INCREMENT,
            ${this.urlColumnName} VARCHAR(255) NOT NULL,
            ${this.titleColumnName} VARCHAR(255),
            ${this.descriptionColumnName} TEXT,
            ${this.rawHtmlColumnName} LONGTEXT,
            PRIMARY KEY (id),
            UNIQUE KEY ${this.urlColumnName} (${this.urlColumnName})
          ) ENGINE=InnoDB;
        `);
      } finally {
        conn.release();
      }
    };

    const createQueueTable = async () => {
      const conn = await this.pool.getConnection();
      try {
        await conn.query(`
          CREATE TABLE IF NOT EXISTS ${this.queueTableName} (
            id INT(11) NOT NULL AUTO_INCREMENT,
            ${this.urlColumnName} VARCHAR(255) NOT NULL,
            ${this.isCrawledColumnName} BOOLEAN DEFAULT false,
            PRIMARY KEY (id),
            UNIQUE KEY ${this.urlColumnName} (${this.urlColumnName})
          ) ENGINE=InnoDB;
        `);
      } finally {
        conn.release();
      }
    };

    await createPagesTable();
    await createQueueTable();

    this.queueUrls([this.targetUrl]);
  }

  private async queueUrls(urls: string[]) {
    const conn = await this.pool.getConnection();
    try {
      const values = urls.map((url) => `("${url}", false)`).join(", ");
      await conn.query(
        `INSERT IGNORE INTO ${this.queueTableName} (${this.urlColumnName}, ${this.isCrawledColumnName}) VALUES ${values}`
      );
    } finally {
      conn.release();
    }
  }

  private async getNextUrl(): Promise<string | null> {
    const conn = await this.pool.getConnection();
    try {
      const [row] = await conn.query(
        `SELECT ${this.urlColumnName} FROM ${this.queueTableName} WHERE ${this.isCrawledColumnName} = false LIMIT 1 FOR UPDATE`
      );
      if (row && row[this.urlColumnName as keyof typeof row]) {
        return row[this.urlColumnName as keyof typeof row];
      } else {
        return null;
      }
    } finally {
      conn.release();
    }
  }

  private async markUrlAsCrawled(url: string) {
    const conn = await this.pool.getConnection();
    try {
      await conn.query(
        `UPDATE ${this.queueTableName} SET ${this.isCrawledColumnName} = true WHERE ${this.urlColumnName} = ?`,
        [url]
      );
    } finally {
      conn.release();
    }
  }

  private async crawl(error: any, res: Crawler.CrawlerRequestResponse, done: Function) {
    if (error) {
      console.error(error);
    } else {
      console.log(`Crawled ${res.options.uri}`);
      const $ = res.$;
      const title = $(this.titleSelector).text();
      const description = $(this.descriptionSelector).attr("content");
      const url = res.options.uri;
      const rawHtml = res.body;
      const conn = await this.pool.getConnection();
      try {
        await conn.query(
          `INSERT INTO ${this.tableName} (${this.urlColumnName}, ${this.titleColumnName}, ${this.descriptionColumnName}, ${this.rawHtmlColumnName}) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE ${this.titleColumnName}=?, ${this.descriptionColumnName}=?, ${this.rawHtmlColumnName}=?`,
          [url, title, description, rawHtml, title, description, rawHtml]
        );
        if (res.options.depth < this.maxDepth) {
          $("a[href^=" + this.targetUrl + "]").each((i, el) => {
            const link = $(el).attr("href");
            if (link) {
              this.queueUrls([link]);
            }
          });
        }
      } finally {
        conn.release();
      }
      // if url is not string, throw error
      if (typeof url !== "string") {
        throw new Error("url is not string");
      }
      
      await this.markUrlAsCrawled(url);
    }
    done();
    const nextUrl = await this.getNextUrl();
    if (nextUrl) {
      this.crawler.queue(nextUrl);
    }
  }
}

export default WebCrawler