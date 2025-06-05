{{config(materialized="table")}}

WITH match_ids AS (
    SELECT
        -- Creating match_id in match_info the same way to ensure consistency
        cmd.match_id, 
        cmd.innings,
        cmd.super_over,
        t.team_id as team,
        cmd.over_number,
        cmd.delivery_number,
        bat.player_id as batter,
        bowl.player_id as bowler,
        non_str.player_id as non_striker,
        rin.player_id as replacement_in,
        rout.player_id as replacement_out,
        rteam.team_id as replacement_team,
        cmd.replacement_reason,
        cmd.runs_batter,
        cmd.runs_extras,
        cmd.runs_total,
        cmd.powerplay,
        cmd.wicket_kind,
        wout.player_id as wicket_player_out,
        wf1.player_id as wicket_fielder_1,
        wf2.player_id as wicket_fielder_2,
        cmd.extras_wides,
        cmd.extras_noballs,
        cmd.extras_byes,
        cmd.extras_legbyes,
        rev_team.team_id as review_by,
        rev_ump.umpire_id as review_umpire,
        rev_bat.player_id as review_batter,
        cmd.review_decision,
        cmd.review_type,
        cmd.review_umpires_call
    FROM {{ source('raw', 'IPL_RAW_DATA') }} as cmd
    LEFT JOIN {{ref('team_dimension')}} AS t
        ON MD5(concat(COALESCE(cmd.team, 'NULL'), '|', COALESCE(cmd.team_type, 'NULL'))) = t.team_id
    LEFT JOIN {{ref('team_dimension')}} AS rteam
        ON MD5(concat(COALESCE(cmd.replacement_team, 'NULL'), '|', COALESCE(cmd.team_type, 'NULL'))) = rteam.team_id
    LEFT JOIN {{ref('team_dimension')}} AS rev_team
        ON MD5(concat(COALESCE(cmd.review_by, 'NULL'), '|', COALESCE(cmd.team_type, 'NULL'))) = rev_team.team_id
    LEFT JOIN {{ref('players_dimension')}} AS bat 
        ON bat.player_name = cmd.batter
    LEFT JOIN {{ref('players_dimension')}} AS bowl
        ON bowl.player_name = cmd.bowler
    LEFT JOIN {{ref('players_dimension')}} AS non_str
        ON non_str.player_name = cmd.non_striker
    LEFT JOIN {{ref('players_dimension')}} AS rin 
        ON rin.player_name = cmd.replacement_in
    LEFT JOIN {{ref('players_dimension')}} AS rout
        ON rout.player_name = cmd.replacement_out
    LEFT JOIN {{ref('players_dimension')}} AS wout 
        ON wout.player_name = cmd.wicket_player_out
    LEFT JOIN {{ref('players_dimension')}} AS wf1
        ON wf1.player_name = cmd.wicket_fielder_1
    LEFT JOIN {{ref('players_dimension')}} AS wf2
        ON wf2.player_name = cmd.wicket_fielder_2
    LEFT JOIN {{ref('players_dimension')}} AS rev_bat
        ON rev_bat.player_name = cmd.review_batter
    LEFT JOIN {{ref('umpires_dimension')}} AS rev_ump
        ON rev_ump.umpire_name = cmd.review_umpire
)

SELECT DISTINCT
    d.match_id,
    d.innings,
    d.super_over,
    d.team,
    d.over_number,
    d.delivery_number,
    d.batter,
    d.bowler,
    d.non_striker,
    d.replacement_in,
    d.replacement_out,
    d.replacement_team,
    d.replacement_reason,
    d.runs_batter,
    d.runs_extras,
    d.runs_total,
    d.powerplay,
    d.wicket_kind,
    d.wicket_player_out,
    d.wicket_fielder_1,
    d.wicket_fielder_2,
    d.extras_wides,
    d.extras_noballs,
    d.extras_byes,
    d.extras_legbyes,
    d.review_by,
    d.review_umpire,
    d.review_batter,
    d.review_decision,
    d.review_type,
    d.review_umpires_call
FROM
    match_ids AS d
WHERE
    d.delivery_number IS NOT NULL