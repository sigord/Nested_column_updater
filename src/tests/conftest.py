"""
conftest.py
"""
#TODO
#https://towardsdatascience.com/testing-best-practices-for-machine-learning-libraries-41b7d0362c95

@pytest.fixture(scope="session")
def spark_session(request):
    """ fixture for creating a spark context
    Args:
    request: pytest.FixtureRequest object
    """
    spark = (
        SparkSession
        .builder
        .master("local[4]")
        .appName("testing-something")
        .getOrCreate()
    )
    request.addfinalizer(lambda: spark.sparkContext.stop())
    return spark