create table regiao (
    id numeric(10) not null,
    nome varchar(100) not null);

alter table regiao
  add constraint regiao_pk
  primary key (id);

create table solicitacao (
    id numeric(10) not null,
    regiao_id numeric(10) not null,
    solicitado_em timestamp(6) not null,
    atendido_em timestamp(6) not null,
    valor numeric(10,02) not null);

alter table solicitacao
  add constraint solicitacao_pk
  primary key (id);

alter table solicitacao
  add constraint solicitacao_regiao_fk
  foreign key (regiao_id)
  references regiao;

