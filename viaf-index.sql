CREATE INDEX viaf_author_id_idx ON viaf_author (viaf_au_id);
CREATE INDEX viaf_author_name_idx ON viaf_author (viaf_au_name);
ALTER TABLE viaf_author_name ADD FOREIGN KEY viaf_au_id REFERENCES viaf_author;

DELETE FROM viaf_author_name WHERE viaf_au_name_source = 'SYNTH';
INSERT INTO viaf_author_name (viaf_au_id, viaf_au_name, viaf_au_name_source, viaf_au_name_dates)
SELECT viaf_au_id, regexp_replace(regexp_replace(viaf_au_name, ',$', ''), '^(.*), (.*)', '\2 \1'), 'SYNTH', viaf_au_name_dates
FROM viaf_author_name
WHERE viaf_au_name LIKE '%,%';

CREATE INDEX viaf_gender_id_idx ON viaf_gender (viaf_au_id);
ALTER TABLE viaf_author_gender ADD FOREIGN KEY viaf_au_id REFERENCES viaf_author;