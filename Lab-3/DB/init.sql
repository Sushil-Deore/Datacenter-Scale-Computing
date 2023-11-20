
CREATE TABLE IF NOT EXISTS outcometypedim(
    outcome_type_id INT PRIMARY KEY,
    outcome_type VARCHAR(300) UNIQUE NOT NULL
  );

CREATE TABLE IF NOT EXISTS animaltypedim(
    animaltype_id BIGSERIAL PRIMARY KEY,
    animaltype VARCHAR(300)
  );

CREATE TABLE IF NOT EXISTS breeddim(
    breed_id BIGSERIAL PRIMARY KEY,
    breed VARCHAR(300)
  );

CREATE TABLE IF NOT EXISTS sexdim(
  sex_id BIGSERIAL PRIMARY KEY,
  sex VARCHAR(300)
);

CREATE TABLE IF NOT EXISTS colordim(
    color_id BIGSERIAL PRIMARY KEY,
    color VARCHAR(300)
  );

CREATE TABLE IF NOT EXISTS datedim(
    date_id INT PRIMARY KEY,
    date VARCHAR(100) UNIQUE NOT NULL,
    year INT2 UNIQUE NOT NULL,
    month INT2 UNIQUE NOT NULL,
    day INT2 UNIQUE NOT NULL
  );

CREATE TABLE IF NOT EXISTS adoption(
    name TEXT UNIQUE NOT NULL,
    animal_id TEXT UNIQUE NOT NULL,
    is_fixed BOOLEAN NOT NULL,
    sex_id INT REFERENCES sexdim (sex_id),    
    color_id INT REFERENCES colordim (color_id),
    date_id INT REFERENCES datedim (date_id),
    outcometype_id INT REFERENCES outcometypedim (outcometype_id),
    animaltype_id INT REFERENCES animaltypedim (animaltype_id),
    breed_id INT REFERENCES breeddim (breed_id)
);