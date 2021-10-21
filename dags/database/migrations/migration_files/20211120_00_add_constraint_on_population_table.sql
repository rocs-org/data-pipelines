DELETE FROM censusdata.population;
ALTER TABLE censusdata.population DROP CONSTRAINT population_pkey;
ALTER TABLE censusdata.population ADD PRIMARY KEY (nuts, agegroup, sex, year);