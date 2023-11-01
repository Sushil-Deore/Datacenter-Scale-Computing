
CREATE TABLE IF NOT EXISTS outcometypedim(
    outcometype_id BIGSERIAL PRIMARY KEY,
    outcometype VARCHAR(300)
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
    date_id BIGSERIAL PRIMARY KEY,
    datetime DATE
  );

CREATE TABLE IF NOT EXISTS adoption(
    name VARCHAR(300),
    animal_id VARCHAR(300),
    sex_id INT REFERENCES sexdim (sex_id),    
    color_id INT REFERENCES colordim (color_id),
    date_id INT REFERENCES datedim (date_id),
    outcometype_id INT REFERENCES outcometypedim (outcometype_id),
    animaltype_id INT REFERENCES animaltypedim (animaltype_id),
    Breed_id INT REFERENCES breeddim (breed_id)
);