# spark/Dockerfile
FROM apache/spark:3.5.0

# Use slim Java-based Spark image (official) 
# Already has Java and Spark installed
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
WORKDIR /opt/spark

# Expose master UI/ports
EXPOSE 7077 8080