DELETE FROM coronacases.german_counties_incidence;
ALTER TABLE coronacases.german_counties_incidence ADD PRIMARY KEY (date_of_report, nuts3);