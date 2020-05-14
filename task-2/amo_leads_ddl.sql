create table if not exists amo_leads
(
	created_by integer,
	name varchar,
	created_at bigint,
	closest_task_at integer,
	group_id integer,
	responsible_user_id integer,
	id integer not null,
	is_deleted boolean,
	status_id integer,
	closed_at integer,
	pipeline_id integer,
	account_id integer,
	loss_reason_id integer,
	sale real,
	updated_at bigint,
	updated_by integer,
	company varchar,
	contacts integer[]
);

create unique index if not exists amo_leads_id_uindex
	on amo_leads (id);