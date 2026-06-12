--
-- PostgreSQL database dump
--

\restrict TrL7FAWSNb6gOfJogrfQpf62gQlOFtvoyLecw7MaKyw78qrgqLhQupjCdqWpNPe

-- Dumped from database version 18.0
-- Dumped by pg_dump version 18.4 (Debian 18.4-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

-- *not* creating schema, since initdb creates it


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS '';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


--
-- Name: alerts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alerts (
    id integer NOT NULL,
    title character varying(500) NOT NULL,
    message text,
    alert_type character varying(100),
    severity character varying(50),
    source character varying(100),
    planning_application_id integer,
    company_id integer,
    council_id integer,
    scheme_id integer,
    is_read boolean,
    is_actioned boolean,
    actioned_by character varying(255),
    actioned_at timestamp without time zone,
    created_at timestamp without time zone DEFAULT now(),
    type character varying(50),
    entity_type character varying(100),
    entity_id integer
);


--
-- Name: alerts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.alerts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: alerts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.alerts_id_seq OWNED BY public.alerts.id;


--
-- Name: brownfield_sites; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.brownfield_sites (
    id integer NOT NULL,
    reference character varying(255) NOT NULL,
    council_id integer,
    address text,
    postcode character varying(20),
    latitude double precision,
    longitude double precision,
    description text,
    num_units integer,
    status character varying(50),
    scheme_type character varying(50),
    source character varying(50) DEFAULT 'brownfield-register'::character varying,
    source_reference character varying(255),
    raw_data jsonb,
    last_verified_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: brownfield_sites_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.brownfield_sites_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: brownfield_sites_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.brownfield_sites_id_seq OWNED BY public.brownfield_sites.id;


--
-- Name: companies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.companies (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    companies_house_number character varying(20),
    sector character varying(100),
    sub_sector character varying(100),
    company_type character varying(100),
    website text,
    headquarters_address text,
    headquarters_postcode character varying(10),
    phone character varying(30),
    employee_count integer,
    revenue_gbp bigint,
    is_client boolean,
    is_competitor boolean,
    is_target boolean,
    relationship_status character varying(50),
    key_contact_name character varying(255),
    key_contact_email character varying(255),
    key_contact_phone character varying(30),
    key_contact_title character varying(255),
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    normalized_name character varying(500) NOT NULL,
    registered_address text,
    sic_codes jsonb,
    parent_company_id integer,
    is_active boolean DEFAULT true NOT NULL
);


--
-- Name: companies_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.companies_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: companies_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.companies_id_seq OWNED BY public.companies.id;


--
-- Name: company_aliases; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.company_aliases (
    id integer NOT NULL,
    company_id integer NOT NULL,
    alias_name character varying(500) NOT NULL,
    source character varying(50) NOT NULL
);


--
-- Name: company_aliases_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.company_aliases_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: company_aliases_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.company_aliases_id_seq OWNED BY public.company_aliases.id;


--
-- Name: contacts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.contacts (
    id integer NOT NULL,
    company_id integer NOT NULL,
    full_name character varying(255) NOT NULL,
    job_title character varying(255),
    email character varying(255),
    phone character varying(50),
    linkedin_url character varying(512),
    source character varying(100),
    confidence_score double precision,
    last_verified_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: contacts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.contacts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: contacts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.contacts_id_seq OWNED BY public.contacts.id;


--
-- Name: councils; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.councils (
    id integer NOT NULL,
    name character varying(255) NOT NULL,
    region character varying(100),
    portal_type character varying(100),
    portal_url text,
    last_scraped_at timestamp without time zone,
    scrape_frequency_hours integer,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    scraper_class character varying(255),
    active boolean DEFAULT true NOT NULL,
    organisation_entity character varying(20)
);


--
-- Name: councils_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.councils_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: councils_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.councils_id_seq OWNED BY public.councils.id;


--
-- Name: existing_schemes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.existing_schemes (
    id integer CONSTRAINT schemes_id_not_null NOT NULL,
    name character varying(500) CONSTRAINT schemes_name_not_null NOT NULL,
    address text,
    postcode character varying(10),
    council_id integer,
    owner_company_id integer,
    operator_company_id integer,
    scheme_type character varying(100),
    status character varying(100),
    total_units integer,
    unit_mix jsonb,
    amenities jsonb,
    completion_date date,
    contract_start_date date,
    contract_end_date date,
    contract_type character varying(100),
    annual_revenue_gbp bigint,
    occupancy_pct double precision,
    avg_rent_pcm double precision,
    performance_rating double precision,
    nps_score double precision,
    latitude double precision,
    longitude double precision,
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    asset_manager_company_id integer,
    landlord_company_id integer,
    source character varying(100),
    source_reference character varying(500),
    last_verified_at timestamp with time zone,
    data_confidence_score double precision,
    hmlr_title_number character varying(20),
    hmlr_tenure character varying(20),
    lat double precision,
    lng double precision,
    num_units integer,
    satisfaction_score double precision,
    financial_health_score double precision,
    regulatory_rating character varying(50),
    epc_ratings jsonb,
    locked_fields jsonb DEFAULT '{}'::jsonb NOT NULL,
    google_rating real,
    google_review_count integer,
    google_place_id character varying(100),
    google_checked_at timestamp with time zone,
    occupancy_rate real,
    occupancy_checked_at timestamp with time zone,
    arrears_risk_score real,
    arrears_checked_at timestamp with time zone,
    bd_score_breakdown jsonb,
    bd_score real,
    bd_score_updated_at timestamp with time zone
);


--
-- Name: COLUMN existing_schemes.hmlr_title_number; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.existing_schemes.hmlr_title_number IS 'HMLR title number matched from CCOD dataset';


--
-- Name: COLUMN existing_schemes.hmlr_tenure; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.existing_schemes.hmlr_tenure IS 'Freehold or Leasehold from HMLR CCOD';


--
-- Name: pipeline_opportunities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pipeline_opportunities (
    id integer NOT NULL,
    title character varying(500) NOT NULL,
    description text,
    planning_application_id integer,
    company_id integer,
    council_id integer,
    source character varying(100),
    stage character varying(100),
    priority character varying(50),
    estimated_units integer,
    estimated_value_gbp bigint,
    expected_start_date date,
    expected_completion_date date,
    probability_pct double precision,
    assigned_to character varying(255),
    last_activity_date date,
    next_action text,
    next_action_date date,
    notes text,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    assigned_to_user_id integer,
    scheme_id integer,
    bd_score double precision,
    last_contact_date date
);


--
-- Name: pipeline_opportunities_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pipeline_opportunities_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pipeline_opportunities_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pipeline_opportunities_id_seq OWNED BY public.pipeline_opportunities.id;


--
-- Name: planning_applications; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.planning_applications (
    id integer NOT NULL,
    council_id integer NOT NULL,
    reference character varying(500) NOT NULL,
    description text,
    address text,
    postcode character varying(10),
    ward character varying(100),
    applicant_name character varying(255),
    applicant_company_id integer,
    agent_name character varying(255),
    agent_company_id integer,
    application_type character varying(255),
    scheme_type character varying(100) DEFAULT 'Unknown'::character varying NOT NULL,
    status character varying(100),
    decision character varying(100),
    decision_date date,
    submitted_date date,
    validated_date date,
    consultation_end_date date,
    committee_date date,
    total_units integer,
    affordable_units integer,
    commercial_sqm double precision,
    storeys integer,
    epc_rating character varying(5),
    latitude double precision,
    longitude double precision,
    portal_url text,
    documents_url text,
    is_btr boolean,
    is_pbsa boolean,
    is_affordable boolean,
    bd_relevance_score double precision,
    notes text,
    raw_data jsonb,
    source character varying(100),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    num_units integer,
    submission_date date,
    appeal_status character varying(100),
    raw_html text,
    bd_score real,
    bd_score_breakdown jsonb,
    bd_score_updated_at timestamp with time zone
);


--
-- Name: planning_applications_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.planning_applications_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: planning_applications_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.planning_applications_id_seq OWNED BY public.planning_applications.id;


--
-- Name: scheme_change_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scheme_change_log (
    id integer NOT NULL,
    scheme_id integer,
    field_name character varying(100) NOT NULL,
    old_value text,
    new_value text,
    changed_by character varying(255),
    changed_at timestamp with time zone DEFAULT now()
);


--
-- Name: scheme_change_log_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.scheme_change_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: scheme_change_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.scheme_change_log_id_seq OWNED BY public.scheme_change_log.id;


--
-- Name: scheme_change_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scheme_change_logs (
    id integer NOT NULL,
    scheme_id integer NOT NULL,
    field_name character varying(100) NOT NULL,
    old_value text,
    new_value text,
    source character varying(100),
    changed_by character varying(100),
    changed_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: scheme_change_logs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.scheme_change_logs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: scheme_change_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.scheme_change_logs_id_seq OWNED BY public.scheme_change_logs.id;


--
-- Name: scheme_contracts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scheme_contracts (
    id integer NOT NULL,
    scheme_id integer NOT NULL,
    contract_reference character varying(500),
    contract_type character varying(500),
    operator_company_id integer,
    client_company_id integer,
    contract_start_date date,
    contract_end_date date,
    contract_value double precision,
    currency character varying(500) DEFAULT 'GBP'::character varying NOT NULL,
    source character varying(500),
    source_reference character varying(500),
    is_current boolean DEFAULT true NOT NULL,
    raw_data json,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: scheme_contracts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.scheme_contracts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: scheme_contracts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.scheme_contracts_id_seq OWNED BY public.scheme_contracts.id;


--
-- Name: scheme_rents; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scheme_rents (
    id integer NOT NULL,
    scheme_id integer NOT NULL,
    room_type character varying(100),
    rent_per_week double precision,
    rent_per_month double precision,
    currency character varying(3) DEFAULT 'GBP'::character varying NOT NULL,
    academic_year character varying(20),
    contract_length_weeks integer,
    source character varying(100),
    source_reference character varying(500),
    scraped_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: scheme_rents_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.scheme_rents_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: scheme_rents_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.scheme_rents_id_seq OWNED BY public.scheme_rents.id;


--
-- Name: schemes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.schemes_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: schemes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.schemes_id_seq OWNED BY public.existing_schemes.id;


--
-- Name: scraper_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.scraper_runs (
    id integer NOT NULL,
    council_id integer,
    source character varying(100),
    status character varying(50) NOT NULL,
    started_at timestamp without time zone,
    completed_at timestamp without time zone,
    applications_found integer DEFAULT 0 NOT NULL,
    applications_new integer DEFAULT 0 NOT NULL,
    applications_updated integer DEFAULT 0 NOT NULL,
    errors_count integer DEFAULT 0 NOT NULL,
    error_details jsonb,
    duration_seconds double precision,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: scraper_runs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.scraper_runs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: scraper_runs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.scraper_runs_id_seq OWNED BY public.scraper_runs.id;


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id integer NOT NULL,
    email character varying(255) NOT NULL,
    name character varying(255) NOT NULL,
    hashed_password character varying(255) NOT NULL,
    role character varying(50) DEFAULT 'viewer'::character varying NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: COLUMN users.role; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.users.role IS 'admin, bd_manager, bd_analyst, viewer';


--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;


--
-- Name: alerts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts ALTER COLUMN id SET DEFAULT nextval('public.alerts_id_seq'::regclass);


--
-- Name: brownfield_sites id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.brownfield_sites ALTER COLUMN id SET DEFAULT nextval('public.brownfield_sites_id_seq'::regclass);


--
-- Name: companies id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies ALTER COLUMN id SET DEFAULT nextval('public.companies_id_seq'::regclass);


--
-- Name: company_aliases id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_aliases ALTER COLUMN id SET DEFAULT nextval('public.company_aliases_id_seq'::regclass);


--
-- Name: contacts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contacts ALTER COLUMN id SET DEFAULT nextval('public.contacts_id_seq'::regclass);


--
-- Name: councils id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.councils ALTER COLUMN id SET DEFAULT nextval('public.councils_id_seq'::regclass);


--
-- Name: existing_schemes id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.existing_schemes ALTER COLUMN id SET DEFAULT nextval('public.schemes_id_seq'::regclass);


--
-- Name: pipeline_opportunities id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_opportunities ALTER COLUMN id SET DEFAULT nextval('public.pipeline_opportunities_id_seq'::regclass);


--
-- Name: planning_applications id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_applications ALTER COLUMN id SET DEFAULT nextval('public.planning_applications_id_seq'::regclass);


--
-- Name: scheme_change_log id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_change_log ALTER COLUMN id SET DEFAULT nextval('public.scheme_change_log_id_seq'::regclass);


--
-- Name: scheme_change_logs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_change_logs ALTER COLUMN id SET DEFAULT nextval('public.scheme_change_logs_id_seq'::regclass);


--
-- Name: scheme_contracts id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_contracts ALTER COLUMN id SET DEFAULT nextval('public.scheme_contracts_id_seq'::regclass);


--
-- Name: scheme_rents id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_rents ALTER COLUMN id SET DEFAULT nextval('public.scheme_rents_id_seq'::regclass);


--
-- Name: scraper_runs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scraper_runs ALTER COLUMN id SET DEFAULT nextval('public.scraper_runs_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


--
-- Name: alerts alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_pkey PRIMARY KEY (id);


--
-- Name: brownfield_sites brownfield_sites_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.brownfield_sites
    ADD CONSTRAINT brownfield_sites_pkey PRIMARY KEY (id);


--
-- Name: brownfield_sites brownfield_sites_reference_council_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.brownfield_sites
    ADD CONSTRAINT brownfield_sites_reference_council_id_key UNIQUE (reference, council_id);


--
-- Name: companies companies_companies_house_number_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_companies_house_number_key UNIQUE (companies_house_number);


--
-- Name: companies companies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_pkey PRIMARY KEY (id);


--
-- Name: company_aliases company_aliases_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_aliases
    ADD CONSTRAINT company_aliases_pkey PRIMARY KEY (id);


--
-- Name: contacts contacts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contacts
    ADD CONSTRAINT contacts_pkey PRIMARY KEY (id);


--
-- Name: councils councils_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.councils
    ADD CONSTRAINT councils_name_key UNIQUE (name);


--
-- Name: councils councils_organisation_entity_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.councils
    ADD CONSTRAINT councils_organisation_entity_key UNIQUE (organisation_entity);


--
-- Name: councils councils_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.councils
    ADD CONSTRAINT councils_pkey PRIMARY KEY (id);


--
-- Name: pipeline_opportunities pipeline_opportunities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_opportunities
    ADD CONSTRAINT pipeline_opportunities_pkey PRIMARY KEY (id);


--
-- Name: planning_applications planning_applications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_applications
    ADD CONSTRAINT planning_applications_pkey PRIMARY KEY (id);


--
-- Name: scheme_change_log scheme_change_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_change_log
    ADD CONSTRAINT scheme_change_log_pkey PRIMARY KEY (id);


--
-- Name: scheme_change_logs scheme_change_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_change_logs
    ADD CONSTRAINT scheme_change_logs_pkey PRIMARY KEY (id);


--
-- Name: scheme_contracts scheme_contracts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_contracts
    ADD CONSTRAINT scheme_contracts_pkey PRIMARY KEY (id);


--
-- Name: scheme_rents scheme_rents_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_rents
    ADD CONSTRAINT scheme_rents_pkey PRIMARY KEY (id);


--
-- Name: existing_schemes schemes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.existing_schemes
    ADD CONSTRAINT schemes_pkey PRIMARY KEY (id);


--
-- Name: scraper_runs scraper_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scraper_runs
    ADD CONSTRAINT scraper_runs_pkey PRIMARY KEY (id);


--
-- Name: planning_applications uq_application_reference_council; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_applications
    ADD CONSTRAINT uq_application_reference_council UNIQUE (reference, council_id);


--
-- Name: users users_email_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_email_key UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: idx_existing_schemes_bd_score; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_existing_schemes_bd_score ON public.existing_schemes USING btree (bd_score DESC NULLS LAST) WHERE (bd_score IS NOT NULL);


--
-- Name: idx_planning_apps_bd_score; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_planning_apps_bd_score ON public.planning_applications USING btree (bd_score DESC NULLS LAST) WHERE (bd_score IS NOT NULL);


--
-- Name: ix_alerts_alert_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_alerts_alert_type ON public.alerts USING btree (alert_type);


--
-- Name: ix_alerts_is_read; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_alerts_is_read ON public.alerts USING btree (is_read);


--
-- Name: ix_alerts_severity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_alerts_severity ON public.alerts USING btree (severity);


--
-- Name: ix_brownfield_sites_council_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_brownfield_sites_council_id ON public.brownfield_sites USING btree (council_id);


--
-- Name: ix_brownfield_sites_num_units; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_brownfield_sites_num_units ON public.brownfield_sites USING btree (num_units);


--
-- Name: ix_brownfield_sites_postcode; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_brownfield_sites_postcode ON public.brownfield_sites USING btree (postcode);


--
-- Name: ix_companies_is_client; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_is_client ON public.companies USING btree (is_client);


--
-- Name: ix_companies_sector; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_companies_sector ON public.companies USING btree (sector);


--
-- Name: ix_company_aliases_alias_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_company_aliases_alias_name ON public.company_aliases USING btree (alias_name);


--
-- Name: ix_contacts_company_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_contacts_company_id ON public.contacts USING btree (company_id);


--
-- Name: ix_contacts_email; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_contacts_email ON public.contacts USING btree (email);


--
-- Name: ix_councils_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_councils_active ON public.councils USING btree (active);


--
-- Name: ix_councils_region; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_councils_region ON public.councils USING btree (region);


--
-- Name: ix_existing_schemes_arrears_risk_score; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_existing_schemes_arrears_risk_score ON public.existing_schemes USING btree (arrears_risk_score);


--
-- Name: ix_existing_schemes_google_rating; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_existing_schemes_google_rating ON public.existing_schemes USING btree (google_rating);


--
-- Name: ix_existing_schemes_locked_fields; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_existing_schemes_locked_fields ON public.existing_schemes USING gin (locked_fields);


--
-- Name: ix_existing_schemes_occupancy_rate; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_existing_schemes_occupancy_rate ON public.existing_schemes USING btree (occupancy_rate);


--
-- Name: ix_pipeline_opportunities_assigned_to_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_pipeline_opportunities_assigned_to_user_id ON public.pipeline_opportunities USING btree (assigned_to_user_id);


--
-- Name: ix_pipeline_opportunities_priority; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_pipeline_opportunities_priority ON public.pipeline_opportunities USING btree (priority);


--
-- Name: ix_pipeline_opportunities_stage; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_pipeline_opportunities_stage ON public.pipeline_opportunities USING btree (stage);


--
-- Name: ix_planning_applications_council_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_planning_applications_council_id ON public.planning_applications USING btree (council_id);


--
-- Name: ix_planning_applications_is_btr; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_planning_applications_is_btr ON public.planning_applications USING btree (is_btr);


--
-- Name: ix_planning_applications_is_pbsa; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_planning_applications_is_pbsa ON public.planning_applications USING btree (is_pbsa);


--
-- Name: ix_planning_applications_postcode; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_planning_applications_postcode ON public.planning_applications USING btree (postcode);


--
-- Name: ix_planning_applications_reference; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_planning_applications_reference ON public.planning_applications USING btree (reference);


--
-- Name: ix_planning_applications_scheme_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_planning_applications_scheme_type ON public.planning_applications USING btree (scheme_type);


--
-- Name: ix_planning_applications_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_planning_applications_status ON public.planning_applications USING btree (status);


--
-- Name: ix_planning_applications_submission_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_planning_applications_submission_date ON public.planning_applications USING btree (submission_date);


--
-- Name: ix_planning_applications_submitted_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_planning_applications_submitted_date ON public.planning_applications USING btree (submitted_date);


--
-- Name: ix_scheme_change_logs_changed_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scheme_change_logs_changed_at ON public.scheme_change_logs USING btree (changed_at);


--
-- Name: ix_scheme_change_logs_field_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scheme_change_logs_field_name ON public.scheme_change_logs USING btree (field_name);


--
-- Name: ix_scheme_change_logs_scheme_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scheme_change_logs_scheme_id ON public.scheme_change_logs USING btree (scheme_id);


--
-- Name: ix_scheme_contracts_end_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scheme_contracts_end_date ON public.scheme_contracts USING btree (contract_end_date);


--
-- Name: ix_scheme_contracts_is_current; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scheme_contracts_is_current ON public.scheme_contracts USING btree (is_current);


--
-- Name: ix_scheme_contracts_scheme_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scheme_contracts_scheme_id ON public.scheme_contracts USING btree (scheme_id);


--
-- Name: ix_scheme_contracts_source_reference; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scheme_contracts_source_reference ON public.scheme_contracts USING btree (source_reference);


--
-- Name: ix_scheme_rents_scheme_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scheme_rents_scheme_id ON public.scheme_rents USING btree (scheme_id);


--
-- Name: ix_scheme_rents_scheme_room; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scheme_rents_scheme_room ON public.scheme_rents USING btree (scheme_id, room_type);


--
-- Name: ix_schemes_asset_manager_company_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_schemes_asset_manager_company_id ON public.existing_schemes USING btree (asset_manager_company_id);


--
-- Name: ix_schemes_council_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_schemes_council_id ON public.existing_schemes USING btree (council_id);


--
-- Name: ix_schemes_hmlr_title_number; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_schemes_hmlr_title_number ON public.existing_schemes USING btree (hmlr_title_number);


--
-- Name: ix_schemes_landlord_company_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_schemes_landlord_company_id ON public.existing_schemes USING btree (landlord_company_id);


--
-- Name: ix_schemes_last_verified_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_schemes_last_verified_at ON public.existing_schemes USING btree (last_verified_at);


--
-- Name: ix_schemes_scheme_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_schemes_scheme_type ON public.existing_schemes USING btree (scheme_type);


--
-- Name: ix_schemes_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_schemes_source ON public.existing_schemes USING btree (source);


--
-- Name: ix_schemes_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_schemes_status ON public.existing_schemes USING btree (status);


--
-- Name: ix_scraper_runs_council_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scraper_runs_council_id ON public.scraper_runs USING btree (council_id);


--
-- Name: ix_scraper_runs_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scraper_runs_source ON public.scraper_runs USING btree (source);


--
-- Name: ix_scraper_runs_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_scraper_runs_status ON public.scraper_runs USING btree (status);


--
-- Name: ix_users_email; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_users_email ON public.users USING btree (email);


--
-- Name: alerts alerts_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id);


--
-- Name: alerts alerts_council_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_council_id_fkey FOREIGN KEY (council_id) REFERENCES public.councils(id);


--
-- Name: alerts alerts_planning_application_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_planning_application_id_fkey FOREIGN KEY (planning_application_id) REFERENCES public.planning_applications(id);


--
-- Name: alerts alerts_scheme_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_scheme_id_fkey FOREIGN KEY (scheme_id) REFERENCES public.existing_schemes(id);


--
-- Name: brownfield_sites brownfield_sites_council_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.brownfield_sites
    ADD CONSTRAINT brownfield_sites_council_id_fkey FOREIGN KEY (council_id) REFERENCES public.councils(id) ON DELETE SET NULL;


--
-- Name: companies companies_parent_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_parent_company_id_fkey FOREIGN KEY (parent_company_id) REFERENCES public.companies(id);


--
-- Name: company_aliases company_aliases_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.company_aliases
    ADD CONSTRAINT company_aliases_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: contacts contacts_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contacts
    ADD CONSTRAINT contacts_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id) ON DELETE CASCADE;


--
-- Name: pipeline_opportunities pipeline_opportunities_assigned_to_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_opportunities
    ADD CONSTRAINT pipeline_opportunities_assigned_to_user_id_fkey FOREIGN KEY (assigned_to_user_id) REFERENCES public.users(id) ON DELETE SET NULL;


--
-- Name: pipeline_opportunities pipeline_opportunities_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_opportunities
    ADD CONSTRAINT pipeline_opportunities_company_id_fkey FOREIGN KEY (company_id) REFERENCES public.companies(id);


--
-- Name: pipeline_opportunities pipeline_opportunities_council_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_opportunities
    ADD CONSTRAINT pipeline_opportunities_council_id_fkey FOREIGN KEY (council_id) REFERENCES public.councils(id);


--
-- Name: pipeline_opportunities pipeline_opportunities_planning_application_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_opportunities
    ADD CONSTRAINT pipeline_opportunities_planning_application_id_fkey FOREIGN KEY (planning_application_id) REFERENCES public.planning_applications(id);


--
-- Name: pipeline_opportunities pipeline_opportunities_scheme_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pipeline_opportunities
    ADD CONSTRAINT pipeline_opportunities_scheme_id_fkey FOREIGN KEY (scheme_id) REFERENCES public.existing_schemes(id) ON DELETE SET NULL;


--
-- Name: planning_applications planning_applications_agent_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_applications
    ADD CONSTRAINT planning_applications_agent_company_id_fkey FOREIGN KEY (agent_company_id) REFERENCES public.companies(id);


--
-- Name: planning_applications planning_applications_applicant_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_applications
    ADD CONSTRAINT planning_applications_applicant_company_id_fkey FOREIGN KEY (applicant_company_id) REFERENCES public.companies(id);


--
-- Name: planning_applications planning_applications_council_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.planning_applications
    ADD CONSTRAINT planning_applications_council_id_fkey FOREIGN KEY (council_id) REFERENCES public.councils(id);


--
-- Name: scheme_change_log scheme_change_log_scheme_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_change_log
    ADD CONSTRAINT scheme_change_log_scheme_id_fkey FOREIGN KEY (scheme_id) REFERENCES public.existing_schemes(id) ON DELETE CASCADE;


--
-- Name: scheme_change_logs scheme_change_logs_scheme_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_change_logs
    ADD CONSTRAINT scheme_change_logs_scheme_id_fkey FOREIGN KEY (scheme_id) REFERENCES public.existing_schemes(id) ON DELETE CASCADE;


--
-- Name: scheme_contracts scheme_contracts_client_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_contracts
    ADD CONSTRAINT scheme_contracts_client_company_id_fkey FOREIGN KEY (client_company_id) REFERENCES public.companies(id) ON DELETE SET NULL;


--
-- Name: scheme_contracts scheme_contracts_operator_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_contracts
    ADD CONSTRAINT scheme_contracts_operator_company_id_fkey FOREIGN KEY (operator_company_id) REFERENCES public.companies(id) ON DELETE SET NULL;


--
-- Name: scheme_contracts scheme_contracts_scheme_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_contracts
    ADD CONSTRAINT scheme_contracts_scheme_id_fkey FOREIGN KEY (scheme_id) REFERENCES public.existing_schemes(id) ON DELETE CASCADE;


--
-- Name: scheme_rents scheme_rents_scheme_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scheme_rents
    ADD CONSTRAINT scheme_rents_scheme_id_fkey FOREIGN KEY (scheme_id) REFERENCES public.existing_schemes(id) ON DELETE CASCADE;


--
-- Name: existing_schemes schemes_asset_manager_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.existing_schemes
    ADD CONSTRAINT schemes_asset_manager_company_id_fkey FOREIGN KEY (asset_manager_company_id) REFERENCES public.companies(id) ON DELETE SET NULL;


--
-- Name: existing_schemes schemes_council_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.existing_schemes
    ADD CONSTRAINT schemes_council_id_fkey FOREIGN KEY (council_id) REFERENCES public.councils(id);


--
-- Name: existing_schemes schemes_landlord_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.existing_schemes
    ADD CONSTRAINT schemes_landlord_company_id_fkey FOREIGN KEY (landlord_company_id) REFERENCES public.companies(id) ON DELETE SET NULL;


--
-- Name: existing_schemes schemes_operator_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.existing_schemes
    ADD CONSTRAINT schemes_operator_company_id_fkey FOREIGN KEY (operator_company_id) REFERENCES public.companies(id);


--
-- Name: existing_schemes schemes_owner_company_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.existing_schemes
    ADD CONSTRAINT schemes_owner_company_id_fkey FOREIGN KEY (owner_company_id) REFERENCES public.companies(id);


--
-- Name: scraper_runs scraper_runs_council_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.scraper_runs
    ADD CONSTRAINT scraper_runs_council_id_fkey FOREIGN KEY (council_id) REFERENCES public.councils(id);


--
-- PostgreSQL database dump complete
--

\unrestrict TrL7FAWSNb6gOfJogrfQpf62gQlOFtvoyLecw7MaKyw78qrgqLhQupjCdqWpNPe

