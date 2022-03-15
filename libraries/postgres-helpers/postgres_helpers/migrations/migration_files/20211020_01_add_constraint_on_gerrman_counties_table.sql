DELETE FROM censusdata.german_counties_info;
ALTER TABLE censusdata.german_counties_info DROP CONSTRAINT german_counties_info_pkey;
ALTER TABLE censusdata.german_counties_info ADD PRIMARY KEY (german_id);