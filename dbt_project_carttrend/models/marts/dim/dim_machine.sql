-- :file_folder: models/marts/dim/dim_machine.sql
SELECT DISTINCT
  id_machine,
  type_machine
FROM {{ ref('stg_entrepots_machine') }}