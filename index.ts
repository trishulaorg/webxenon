import WebCrawler from "./WebCrawler";
import type { WebCrawlerOptions} from "./WebCrawler"
import dotenv from "dotenv";

dotenv.config();

function main() {
  const {
    DB_HOST,
    DB_USER,
    DB_PASSWORD,
    DB_NAME,
    TARGET_URL,
    MAX_CONNECTIONS,
    RATE_LIMIT,
    USER_AGENT,
    TABLE_NAME,
    URL_COLUMN_NAME,
    TITLE_COLUMN_NAME,
    DESCRIPTION_COLUMN_NAME,
    RAW_HTML_COLUMN_NAME,
    TITLE_SELECTOR,
    DESCRIPTION_SELECTOR,
    MAX_DEPTH,
    QUEUE_TABLE_NAME,
    IS_CRAWLED_COLUMN_NAME,
  } = process.env;

  if (!DB_HOST || !DB_USER || !DB_PASSWORD || !DB_NAME || !TARGET_URL) {
    console.error("Missing required configuration variables.");
    process.exit(1);
  }

  const options: WebCrawlerOptions = {
    maxConnections: MAX_CONNECTIONS ? parseInt(MAX_CONNECTIONS, 10) : undefined,
    rateLimit: RATE_LIMIT ? parseInt(RATE_LIMIT, 10) : undefined,
    userAgent: USER_AGENT,
    tableName: TABLE_NAME,
    urlColumnName: URL_COLUMN_NAME,
    titleColumnName: TITLE_COLUMN_NAME,
    descriptionColumnName: DESCRIPTION_COLUMN_NAME,
    rawHtmlColumnName: RAW_HTML_COLUMN_NAME,
    titleSelector: TITLE_SELECTOR,
    descriptionSelector: DESCRIPTION_SELECTOR,
    maxDepth: MAX_DEPTH ? parseInt(MAX_DEPTH, 10) : undefined,
    queueTableName: QUEUE_TABLE_NAME,
    isCrawledColumnName: IS_CRAWLED_COLUMN_NAME,
  };

  const crawler = new WebCrawler(
    DB_HOST,
    DB_USER,
    DB_PASSWORD,
    DB_NAME,
    TARGET_URL,
    options
  );
  crawler.start();
}

main();
