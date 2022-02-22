FROM openjdk:11.0.13-jre
RUN apt-get update
RUN apt-get install procps -y
RUN wget https://dlcdn.apache.org/spark/spark-3.0.3/spark-3.0.3-bin-hadoop2.7.tgz
RUN tar xvf spark-*
RUN rm spark-3.0.3-bin-hadoop2.7.tgz
ENV SPARK_HOME /spark-3.0.3-bin-hadoop2.7
ENV PATH $PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV CORES 2
ENV MEM 4G
EXPOSE 8581
EXPOSE 8981
COPY ./start.sh .
RUN chmod 111 ./start.sh
CMD ["./start.sh"]