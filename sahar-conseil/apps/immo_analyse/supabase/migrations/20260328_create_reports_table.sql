-- Create reports table for storing generated analysis reports
CREATE TABLE IF NOT EXISTS reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  address TEXT NOT NULL,
  city TEXT NOT NULL,
  postcode TEXT NOT NULL,
  commune_id TEXT,
  latitude DECIMAL(10, 8),
  longitude DECIMAL(11, 8),
  scores JSONB,
  stats JSONB,
  forecast JSONB,
  narrative TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  search_vector tsvector GENERATED ALWAYS AS (to_tsvector('french', coalesce(address, '') || ' ' || coalesce(city, ''))) STORED
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_reports_user_id ON reports(user_id);
CREATE INDEX IF NOT EXISTS idx_reports_created_at ON reports(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_reports_commune_id ON reports(commune_id);
CREATE INDEX IF NOT EXISTS idx_reports_search ON reports USING gin(search_vector);

-- Row Level Security
ALTER TABLE reports ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can view their own reports" ON reports FOR SELECT USING (auth.uid() = user_id);
CREATE POLICY "Users can create their own reports" ON reports FOR INSERT WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can update their own reports" ON reports FOR UPDATE USING (auth.uid() = user_id) WITH CHECK (auth.uid() = user_id);
CREATE POLICY "Users can delete their own reports" ON reports FOR DELETE USING (auth.uid() = user_id);

-- Auto-update timestamp trigger
CREATE OR REPLACE FUNCTION update_reports_updated_at() RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS update_reports_timestamp ON reports;
CREATE TRIGGER update_reports_timestamp BEFORE UPDATE ON reports FOR EACH ROW EXECUTE FUNCTION update_reports_updated_at();
