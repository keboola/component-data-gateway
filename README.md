Keboola Data Gateway
=============

Data Gateway component is providing users with ability to share selected tables from storage in read-only mode with external tools and users.
Among main use cases belongs accessing data from 3rd party BI tools (Tableau, Looker, GoodData etc.).

Prerequisites
=============

- Project needs to be using Snowflake backend, as the component is using Snowflake Reader Account functionality for data provisioning.
- Any 3rd party tool accessing the data needs to support Snowflake Key Pair authentication method.

Features
========

| **Feature**                | **Description**                                                      |
|----------------------------|----------------------------------------------------------------------|
| Row-Based Configuration    | Allows structuring the configuration in rows.                        |
| Key Pair Generator         | Component automatically generates Key Pair for acessing the data.    |
| Copy/Clone Full Load Types | Allows two full load modes, where copy allows data types conversion. |


Configuration
=============

Set Up Credentials
-------
Set up credentials for accessing the data from the external tools.

<img width="1260" height="719" alt=" My Keboola Data Gateway Application 2025-08-14 12-44-58" src="https://github.com/user-attachments/assets/2bfb4cd2-7238-4dcd-b37c-5a184f1feb6e" />

Firstly, the component generates Key Pair, which will be used for authentication. The Private Key is automatically downloaded (based on user's browser settings) and not stored after that anywhere for enhanced security. If the user doesn't download the Private Key immediately, they can initiate the download by clicking the `Download Private Key` button before they initiate Workspace creation. If user needs to access Private Key later on, they need to regenerate the whole Key Pair.

<img width="1101" height="872" alt="Save 2025-08-14 12-46-55" src="https://github.com/user-attachments/assets/f99fca91-0e35-44b0-81e4-3a29ef5263a0" />

<img width="1164" height="702" alt=" My Keboola Data Gateway Application 2025-08-14 12-50-55" src="https://github.com/user-attachments/assets/d35fdf11-f17c-4007-b180-36281c32b025" />

Once user `Create Read-Only Workspace`, the generated Public Key is automatically assigned to the database user with privileges to access the data in the read-only workspace and user is provided with all necessary credentials to connect via any tool supporting Snowflake Key Pair database connection. These credentials are later on accessible by clicking on `Database credentials` in the right menu.

<img width="694" height="401" alt=" My Keboola Data Gateway Application 2025-08-14 12-54-48" src="https://github.com/user-attachments/assets/46aaea07-8c9a-4ed6-bd14-d2b8123a25cc" />


Addig Tables
-------
After the credentials are privisioned, user can add any tables from the storage to be shared through Data Gateway in read-only mode.

<img width="1466" height="668" alt=" My Keboola Data Gateway Application 2025-08-14 13-29-24" src="https://github.com/user-attachments/assets/eb30afff-a564-4b4c-8a29-ab54830438f8" />

When using `Full Load (Copy)` load type, user can change names of the columns and their data type before loading them to read-only workspace.

<img width="1101" height="1087" alt=" recipients 2025-08-14 14-10-38" src="https://github.com/user-attachments/assets/cccc1c22-fcd9-4e44-950d-47ced7dbdb5b" />


Running Component
-------
Running the component will load all selected tables to the read-only workspace, providing mirrored version of the data in the user's storage.

<img width="1464" height="437" alt=" My Keboola Data Gateway Application 2025-08-14 14-12-06" src="https://github.com/user-attachments/assets/066379f5-27e9-4b5b-b784-1077fcb5a2a6" />
