--
-- PostgreSQL database dump
--

-- Dumped from database version 16.1
-- Dumped by pg_dump version 16.1

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
-- Name: regiao; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.regiao (
    id bigint,
    name text
);


ALTER TABLE public.regiao OWNER TO postgres;

--
-- Data for Name: regiao; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.regiao (id, name) FROM stdin;
7	Independência
10	Pioneira
15	Leonardo
6	Califórnia
26	Canaa
24	Alvorada
19	Correntão
25	Anta
21	Flor Do Campo
3	Região Rio Juruena
17	Nova
20	Pirapó
18	Passo Fundo
16	Horizonte
27	Cascavel
23	Primavera
2	Região Norte
4	Região Sul
34	Super Pão
35	Tucunaré 1
36	Tucunaré 2
33	Setor Sul
8	Brasflor
32	Descanso
9	Ciapar
11	Rio Do Sangue
12	Rio Verde
22	Gueno
13	Suissa
29	Mutum
31	Tanguro
30	Onca
14	Triangulo Da Fronteira
28	Darro
1	Arrendamento Ughini's
5	São Domingos
\.


--
-- PostgreSQL database dump complete
--

