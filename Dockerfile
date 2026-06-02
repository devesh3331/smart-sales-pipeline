FROM quay.io/astronomer/astro-runtime:12.6.0

# Install Java for PySpark
USER root
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

USER astro