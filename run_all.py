from create_tables import main as create_db
from etl import main as etl_job

if __name__ == "__main__":
    """
    You can run the project each file create_tables.py and etl.py respectively to test. But for simplicity of testing
    I have created run_all.py to run the both script in single go in respective order.
    """
    print("Begin of Program")
    create_db()
    etl_job()
    print("End of Program")
