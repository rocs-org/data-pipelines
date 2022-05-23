with fin_qtn_sessions_per_day as (

SELECT To_timestamp(completed_at / 1000) :: date AS cdate,
       name                                      AS study_name,
       Count(user_id)                            AS fin_qtn_sessions
FROM   datenspende.questionnaire_session
       inner join datenspende.study_overview
               ON datenspende.questionnaire_session.study =
                  datenspende.study_overview.id
WHERE  completed_at IS NOT NULL
GROUP  BY study_name, cdate
ORDER  BY cdate 

)

select *
from fin_qtn_sessions_per_day
