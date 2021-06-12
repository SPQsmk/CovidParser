CREATE DATABASE covid;

\c covid

CREATE TABLE IF NOT EXISTS regions(
    reg_id   SMALLSERIAL,
    reg_name VARCHAR(255) NOT NULL,
    reg_code VARCHAR(10)  NOT NULL,

    CONSTRAINT pk_region
        PRIMARY KEY (reg_id),
    CONSTRAINT uq_region
        UNIQUE (reg_name, reg_code),
    CONSTRAINT ck_region
        CHECK (reg_name != '' AND reg_code != '')
);

CREATE TABLE IF NOT EXISTS dates(
    date_id      SERIAL,
    posting_date DATE NOT NULL,

    CONSTRAINT pk_date
        PRIMARY KEY (date_id),
    CONSTRAINT uq_date
        UNIQUE (posting_date)
);

CREATE TABLE IF NOT EXISTS summary_data(
    summary_data_id   BIGSERIAL,
    reg_id    INTEGER NOT NULL,
    date_id   INTEGER NOT NULL,
    infected  INTEGER NOT NULL,
    recovered INTEGER NOT NULL,
    deceased  INTEGER NOT NULL,

    CONSTRAINT pk_summary_data
        PRIMARY KEY (summary_data_id),
    CONSTRAINT uq_reg_and_date_summary
        UNIQUE (reg_id, date_id),
    CONSTRAINT fk_reg_id_summary
        FOREIGN KEY (reg_id) REFERENCES regions(reg_id)
            ON UPDATE CASCADE
            ON DELETE CASCADE,
    CONSTRAINT fk_date_id_summary
        FOREIGN KEY (date_id) REFERENCES dates(date_id)
            ON UPDATE CASCADE
            ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION check_reg(_code VARCHAR) RETURNS INTEGER
LANGUAGE plpgsql
AS $$
    BEGIN
        RETURN (SELECT reg_id FROM regions WHERE reg_code = _code);
    END
$$;

CREATE OR REPLACE PROCEDURE add_reg(_name VARCHAR, _code VARCHAR)
LANGUAGE SQL
AS $$
    INSERT INTO regions(reg_name, reg_code)
    VALUES(_name, _code);
$$;

CREATE OR REPLACE FUNCTION check_date(_date DATE) RETURNS INTEGER
LANGUAGE plpgsql
AS $$
    BEGIN
        RETURN (SELECT date_id FROM dates WHERE posting_date = _date);
    END
$$;

CREATE OR REPLACE PROCEDURE add_date(_date DATE)
LANGUAGE SQL
AS $$
    INSERT INTO dates(posting_date)
    VALUES(_date);
$$;

CREATE OR REPLACE FUNCTION get_last_summary_date() RETURNS DATE
LANGUAGE plpgsql
AS $$
    BEGIN
        RETURN (SELECT posting_date FROM dates WHERE date_id = (SELECT max(date_id) FROM summary_data));
    END
$$;

CREATE OR REPLACE FUNCTION check_summary_data(_code VARCHAR, _date DATE) RETURNS INTEGER
LANGUAGE plpgsql
AS $$
    DECLARE
        _reg_id "regions"."reg_id"%type;
        _date_id "dates"."date_id"%type;
    BEGIN
        SELECT regions.reg_id INTO _reg_id FROM regions WHERE regions.reg_code = _code;
        SELECT dates.date_id INTO _date_id FROM dates WHERE dates.posting_date = _date;
        RETURN (SELECT summary_data_id FROM summary_data WHERE date_id = _date_id AND reg_id = _reg_id);
    END
$$;

CREATE OR REPLACE PROCEDURE add_summary_data(_code VARCHAR, _date DATE, _infected INTEGER, _recovered INTEGER, _deceased INTEGER)
LANGUAGE plpgsql
AS $$
    DECLARE
        _reg_id "regions"."reg_id"%type;
        _date_id "dates"."date_id"%type;
    BEGIN
        SELECT regions.reg_id INTO _reg_id FROM regions WHERE regions.reg_code = _code;
        SELECT dates.date_id INTO _date_id FROM dates WHERE dates.posting_date = _date;
        INSERT INTO summary_data(reg_id, date_id, infected, recovered, deceased)
        VALUES(_reg_id, _date_id, _infected, _recovered, _deceased);
    END
$$;

CREATE OR REPLACE FUNCTION get_summary_data()
RETURNS TABLE (reg_name VARCHAR, posting_date DATE, infected INTEGER, recovered INTEGER, deceased INTEGER)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT regions.reg_name, dates.posting_date, summary_data.infected, summary_data.recovered, summary_data.deceased
    FROM summary_data, regions, dates
    WHERE summary_data.reg_id = regions.reg_id AND summary_data.date_id = dates.date_id
    ORDER BY dates.posting_date, regions.reg_name COLLATE "C";
END
$$;