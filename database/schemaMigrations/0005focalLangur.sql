INSERT INTO species (scientific_name, common_name, phylum, taxonomic_class, taxonomic_order, family, genus)
VALUES ('Semnopithecus ajax', 'Kashmir Grey Langur', 'Chordata', 'Mammalia', 'Primates', 'Cercopithecidae', 'Semnopithecus')
ON CONFLICT(scientific_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS focal_field_evidence (
    species_id INTEGER NOT NULL REFERENCES species(id),
    h3_cell TEXT NOT NULL CHECK (length(h3_cell) = 15 AND lower(h3_cell) = h3_cell),
    period_start TEXT NOT NULL CHECK (date(period_start) IS period_start),
    period_end TEXT NOT NULL CHECK (date(period_end) IS period_end AND period_end >= period_start),
    census_count INTEGER CHECK (census_count >= 0),
    threat_coverage INTEGER NOT NULL DEFAULT 0 CHECK (threat_coverage IN (0, 1)),
    source TEXT NOT NULL CHECK (length(trim(source)) > 0),
    CHECK (census_count IS NOT NULL OR threat_coverage = 1),
    PRIMARY KEY (species_id, h3_cell, period_start, period_end, source)
) STRICT;
