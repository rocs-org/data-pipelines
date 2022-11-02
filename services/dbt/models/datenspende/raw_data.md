{% docs vitaldata_desc %}

This table contains the daily vital data of users of the Datenspende App, imported from Thryve and updated daily. The triplet of user, vital type, and date is unique (there can only be one step count for user 12345 on the 26th of June 2022). The table is updated nightly with the data received on the previous day. Sometimes, this also includes data that is older than one day, e.g. when someone connects their wearable to the Internet for the first time in a couple of days.

{% enddocs %}


{% docs questions_desc %}

This table contains the questions that make up the surveys available in the Datenspende app.

{% enddocs %}


{% docs users_desc %}

This table contains the users demographic info.

{% enddocs %}


{% docs questionnaires_desc %}

This table contains the questionnaires we serve to the Datenspende users in the app.

{% enddocs %}


{% docs questionnaire_session_desc %}

This table contains the questionnaire session info created whenever a user enters answers to a questionnaire.

{% enddocs %}


{% docs question_to_questionnaire_desc %}

This table maps the questions from the questions table to the questionnaires from the questionnaires table. Includes the position of the question in the questionnaire, together all three form a unique triplet.

{% enddocs %}


{% docs choice_desc %}

This table maps the possible choices for each question, and their order, to the questions itself. The field element is the primary key and can be used to resolve the answer the user selected (from the answer table) with the text field in this table.

{% enddocs %}


{% docs answers_desc %}

This table connects the answers given to users in response to a question to the choices (via element) for that question, the underlying questionnaire, sutdy, and other metadata. The pair id, element (which is the PK for the choice table) defines the primary key.

{% enddocs %}