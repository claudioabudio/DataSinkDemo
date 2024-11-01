class Log4J:
    def __init__(self, spark):
        rootClass = "guru.learningjournal.spark.examples"
        appName = spark.conf.get("spark.app.name")
        self.logger = spark._jvm.org.apache.logging.log4j.LogManager.getLogger(rootClass+"."+appName)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)