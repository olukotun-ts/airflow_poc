CREATE TABLE IF NOT EXISTS nyt_articles (
    id UUID,
    headline TEXT,
    abstract TEXT,
    lead_paragraph TEXT,
    byline TEXT,
    type TEXT,
    pub_date TIMESTAMP,
    url TEXT
);
