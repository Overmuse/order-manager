SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: allocations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.allocations (
    owner text NOT NULL,
    sub_owner text,
    claim_id uuid,
    lot_id uuid NOT NULL,
    ticker text NOT NULL,
    shares numeric NOT NULL,
    basis numeric NOT NULL
);


--
-- Name: claims; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.claims (
    id uuid NOT NULL,
    strategy text NOT NULL,
    sub_strategy text,
    ticker text NOT NULL,
    amount numeric NOT NULL,
    unit text NOT NULL
);


--
-- Name: dependent_orders; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dependent_orders (
    dependent_id text NOT NULL,
    symbol text NOT NULL,
    qty integer NOT NULL,
    side text NOT NULL,
    order_type text NOT NULL,
    limit_price numeric,
    stop_price numeric,
    time_in_force text NOT NULL,
    extended_hours boolean NOT NULL,
    client_order_id text,
    order_class text NOT NULL
);


--
-- Name: lots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.lots (
    id uuid NOT NULL,
    order_id text NOT NULL,
    ticker text NOT NULL,
    fill_time timestamp with time zone NOT NULL,
    price numeric NOT NULL,
    shares numeric NOT NULL
);


--
-- Name: pending_orders; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pending_orders (
    id text NOT NULL,
    ticker text NOT NULL,
    quantity integer NOT NULL,
    pending_quantity integer NOT NULL
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying(255) NOT NULL
);


--
-- Name: claims claims_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.claims
    ADD CONSTRAINT claims_pkey PRIMARY KEY (id);


--
-- Name: lots lots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.lots
    ADD CONSTRAINT lots_pkey PRIMARY KEY (id);


--
-- Name: pending_orders pending_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pending_orders
    ADD CONSTRAINT pending_orders_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- PostgreSQL database dump complete
--


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20210623223745'),
    ('20210623223850'),
    ('20210623223855'),
    ('20210623223901'),
    ('20210623223910');
