CREATE OR REPLACE VIEW
    dune_user_generated.cow_protocol_retention_data_{{Environment}} (day, retained, hybrid, lost, gone)
AS (
    select * from (
        VALUES {{Values}}
    ) as _
);

select * from dune_user_generated.cow_protocol_retention_data_{{Environment}}