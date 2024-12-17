from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

# Arguments par défaut pour le DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Définir le DAG
with DAG(
    "create_tables_with_insert_and_left_join",
    default_args=default_args,
    description="Un DAG pour créer des tables, insérer des données et effectuer une jointure à gauche (LEFT JOIN) dans PostgreSQL en utilisant SQLExecuteQueryOperator",
    schedule_interval=None,  # Exécution manuelle
    start_date=datetime(2024, 12, 17),
    catchup=False,
) as dag:
    # Tâche pour créer la table "departments"
    create_departments_table = SQLExecuteQueryOperator(
        task_id="create_departments_table",
        sql="""
            DROP TABLE IF EXISTS employees; -- only for testing purpose
            DROP TABLE IF EXISTS departments; -- only for testing purpose
            CREATE TABLE IF NOT EXISTS departments (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            );
        """,
        conn_id="airflow-db",  # Connexion à PostgreSQL
    )

    # Tâche pour créer la table "employees"
    create_employees_table = SQLExecuteQueryOperator(
        task_id="create_employees_table",
        sql="""
            DROP TABLE IF EXISTS employees; -- only for testing purpose
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                department_id INTEGER REFERENCES departments(id),
                age INTEGER,
                salary NUMERIC
            );
        """,
        conn_id="airflow-db",  # Connexion à PostgreSQL
    )

    # Tâche pour insérer des données dans la table "departments"
    insert_departments_data = SQLExecuteQueryOperator(
        task_id="insert_departments_data",
        sql="""
            INSERT INTO departments (name) 
            VALUES 
            ('Engineering'),
            ('Marketing'),
            ('Finance'),
            ('HR'),
            ('Sales');
        """,
        conn_id="airflow-db",  # Connexion à PostgreSQL
    )

    # Tâche pour insérer des données dans la table "employees"
    insert_employees_data = SQLExecuteQueryOperator(
        task_id="insert_employees_data",
        sql="""
            INSERT INTO employees (name, department_id, age, salary) 
            VALUES 
            ('John Doe', 1, 34, 65000),
            ('Jane Smith', 2, 29, 48000),
            ('Bob Johnson', 3, 42, 72000),
            ('Alice Brown', 4, 26, 50000),
            ('Tom White', 5, 37, 60000),
            ('Susan Green', 1, 31, 58000),
            ('James Black', 3, 45, 75000),
            ('Emma Wilson', 1, 28, 54000),
            ('Michael Lee', 2, 39, 69000),
            ('Sarah Brown', 3, 33, 62000);
        """,
        conn_id="airflow-db",  # Connexion à PostgreSQL
    )

    # Tâche pour créer la table "join_result" pour stocker les résultats du LEFT JOIN
    create_join_result_table = SQLExecuteQueryOperator(
        task_id="create_join_result_table",
        sql="""
            DROP TABLE IF EXISTS join_result;
            CREATE TABLE IF NOT EXISTS join_result (
                department_name VARCHAR(100),
                employee_name VARCHAR(100),
                salary NUMERIC,
                avg_salary NUMERIC
            );
        """,
        conn_id="airflow-db",  # Connexion à PostgreSQL
    )

    # # Tâche pour effectuer la jointure à gauche (LEFT JOIN) entre les employés et les départements
    # left_join_query = SQLExecuteQueryOperator(
    #     task_id="left_join_employees_departments",
    #     sql="""
    #     SELECT 
    #         a.department_name,
    #         b.employee_name,
    #         b.salary,
    #         c.avg_salary
    #     FROM 
    #         (  -- Première sous-requête : récupère les départements
    #             SELECT 
    #                 id, 
    #                 name AS department_name 
    #             FROM departments
    #         ) a
    #     LEFT JOIN 
    #         (  -- Deuxième sous-requête : récupère les employés et leur salaire
    #             SELECT 
    #                 id AS employee_id,
    #                 name AS employee_name,
    #                 salary,
    #                 department_id
    #             FROM employees
    #         ) b 
    #         ON a.id = b.department_id
    #     LEFT JOIN 
    #         (  -- Troisième sous-requête : récupère la moyenne des salaires par département
    #             SELECT 
    #                 department_id, 
    #                 AVG(salary) AS avg_salary
    #             FROM employees
    #             GROUP BY department_id
    #         ) c 
    #         ON a.id = c.department_id;
    #     """,
    #     conn_id="airflow-db",  # Connexion à PostgreSQL
    #     autocommit=True,  # Commit automatique de la transaction
    # )

    # Tâche pour insérer les résultats du LEFT JOIN dans la table "join_result"
    insert_join_result = SQLExecuteQueryOperator(
        task_id="insert_join_result",
        sql="""
            INSERT INTO join_result (department_name, employee_name, salary, avg_salary)
            SELECT 
                a.department_name,
                b.employee_name,
                b.salary,
                c.avg_salary
            FROM 
                (  -- Première sous-requête : récupère les départements
                    SELECT 
                        id, 
                        name AS department_name 
                    FROM departments
                ) a
            LEFT JOIN 
                (  -- Deuxième sous-requête : récupère les employés et leur salaire
                    SELECT 
                        id AS employee_id,
                        name AS employee_name,
                        salary,
                        department_id
                    FROM employees
                ) b 
                ON a.id = b.department_id
            LEFT JOIN 
                (  -- Troisième sous-requête : récupère la moyenne des salaires par département
                    SELECT 
                        department_id, 
                        AVG(salary) AS avg_salary
                    FROM employees
                    GROUP BY department_id
                ) c 
                ON a.id = c.department_id;
        """,
        conn_id="airflow-db",  # Connexion à PostgreSQL
    )

    # Définir les dépendances des tâches (créer les tables, insérer les données, puis effectuer la jointure à gauche)
    (
        create_departments_table
        >> create_employees_table
        >> insert_departments_data
        >> insert_employees_data
        # >> left_join_query
        >> create_join_result_table
        >> insert_join_result
    )
