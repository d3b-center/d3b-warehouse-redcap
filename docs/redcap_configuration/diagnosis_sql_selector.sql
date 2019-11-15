/*
This code populates a SQL selector dropdown with the human-readable values 
recorded in the diagnosis fields named date_of_initial_diagnosis, 
diagnosis_type, and init_path_diag with " :: " as a delimiter between them.
*/

SELECT MAX(IF(a.instance IS NULL, "1", a.instance)) AS raw_value_repeating_instance, CONCAT_WS(
        " ",
        CONCAT(MAX(IF(a.field_name="date_of_initial_diagnosis", a.value, NULL)), " ::"),
        CONCAT(
            TRIM(SUBSTRING_INDEX(
                SUBSTRING_INDEX(
                    CONCAT(" ", MAX(IF(m.field_name="diagnosis_type", m.ee, NULL))),
                    CONCAT(" ", MAX(IF(a.field_name="diagnosis_type", a.value, NULL)), ", "),
                    -1
                ), CONCAT(CHAR(92), "n"), 1
            )),
            " ::"
        ),
        GROUP_CONCAT(
            TRIM(SUBSTRING_INDEX(
                SUBSTRING_INDEX(
                    CONCAT(" ", IF(m.field_name="init_path_diag", m.ee, NULL)),
                    CONCAT(" ", IF(a.field_name="init_path_diag", a.value, NULL), ", "),
                    -1
                ), CONCAT(CHAR(92), "n"), 1
            ))
            SEPARATOR " / "
        )
    ) AS combined_labels
FROM rcp_prd_dbhi.redcap_data a, (
        SELECT field_name, CONCAT(" ", element_enum) ee
        FROM rcp_prd_dbhi.redcap_metadata
        WHERE
            project_id = [project-id]
            AND (field_name="diagnosis_type" OR field_name="init_path_diag")
    ) m
WHERE a.project_id = [project-id]
    AND a.event_id = 'YOU MUST CUSTOMIZE THIS'
    AND a.record = [record-name]
GROUP BY a.instance
HAVING COALESCE(
    GROUP_CONCAT(IF(a.field_name="diagnosis_complete", value, NULL)),
    ""
)
REGEXP "2";
