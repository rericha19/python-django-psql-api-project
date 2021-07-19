from django.db import migrations


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.RunSQL(
            "ALTER TABLE ov.or_podanie_issues DROP COLUMN IF EXISTS company_id; "
            "ALTER TABLE ov.likvidator_issues DROP COLUMN IF EXISTS company_id; "
            "ALTER TABLE ov.konkurz_vyrovnanie_issues DROP COLUMN IF EXISTS company_id; "
            "ALTER TABLE ov.znizenie_imania_issues DROP COLUMN IF EXISTS company_id; "
            "ALTER TABLE ov.konkurz_restrukturalizacia_actors DROP COLUMN IF EXISTS company_id; "
            ""
            "drop table if exists ov.companies; "
            "create table ov.companies ( "
            "	cin bigint PRIMARY KEY not NULL, "
            "	name character varying, "
            "	br_section character varying, "
            "	address_line character varying, "
            "	last_update timestamp without time zone, "
            "	created_at timestamp without time zone, "
            "	updated_at timestamp without time zone "
            "); "

            "insert into ov.companies "
            "select cin, corporate_body_name, br_section, "
            "case when address_line is not null "
            "   then address_line "
            "   else concat(street, ' ', postal_code, ' ', city) "
            "   end, "
            "updated_at, now(), now() "
            "from ( "
            "   select *, "
            "    rank() over (partition by cin order by updated_at desc) "
            "    from ov.or_podanie_issues "
            ") t "
            "where rank = 1 and cin is not null "
            "on conflict do nothing;"

            "insert into ov.companies "
            "select cin, corporate_body_name, br_section, "
            "   concat(street, ' ', building_number, ', ', postal_code, ' ', city), updated_at, now(), now() "
            "from ( "
            "   select *, "
            "    rank() over (partition by cin order by updated_at desc) "
            "    from ov.likvidator_issues "
            ") t "
            "where rank = 1 and cin is not null "
            "on conflict do nothing; "

            "insert into ov.companies "
            "select cin, corporate_body_name, null,"
            "    concat(street, ' ', building_number, ', ', postal_code, ' ', city), updated_at, now(), now() "
            "from ( "
            "   select *, "
            "    rank() over (partition by cin order by updated_at desc) "
            "    from ov.konkurz_vyrovnanie_issues "
            ") t "
            "where rank = 1 and cin is not null "
            "on conflict do nothing; "

            "insert into ov.companies "
            "select cin, corporate_body_name, br_section, "
            "   concat(street, ' ', building_number, ', ', postal_code, ' ', city), updated_at, now(), now() "
            "from ( "
            "   select *, "
            "    rank() over (partition by cin order by updated_at desc) "
            "    from ov.znizenie_imania_issues "
            ") t "
            "where rank = 1 and cin is not null "
            "on conflict do nothing; "

            "insert into ov.companies "
            "select cin, corporate_body_name, null, "
            "   concat(street, ' ', building_number, ', ', postal_code, ' ', city), updated_at, now(), now() "
            "from ( "
            "   select *, "
            "    rank() over (partition by cin order by updated_at desc) "
            "    from ov.konkurz_restrukturalizacia_actors "
            ") t "
            "where rank = 1 and cin is not null "
            "on conflict do nothing; "

            "ALTER TABLE ov.or_podanie_issues ADD COLUMN company_id bigint;"
            "ALTER TABLE ov.likvidator_issues ADD COLUMN company_id bigint;"
            "ALTER TABLE ov.konkurz_vyrovnanie_issues ADD COLUMN company_id bigint;"
            "ALTER TABLE ov.znizenie_imania_issues ADD COLUMN company_id bigint;"
            "ALTER TABLE ov.konkurz_restrukturalizacia_actors ADD COLUMN company_id bigint;"

            "UPDATE ov.or_podanie_issues SET company_id = cin;"
            "UPDATE ov.likvidator_issues SET company_id = cin;"
            "UPDATE ov.konkurz_vyrovnanie_issues SET company_id = cin;"
            "UPDATE ov.znizenie_imania_issues SET company_id = cin;"
            "UPDATE ov.konkurz_restrukturalizacia_actors SET company_id = cin;"
        )
    ]
