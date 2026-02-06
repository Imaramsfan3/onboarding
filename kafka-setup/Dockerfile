FROM confluentinc/cp-kafka:7.5.0
COPY create-topics.sh /create-topics.sh
RUN sed -i 's/\r$//' /create-topics.sh && chmod +x /create-topics.sh
CMD ["/create-topics.sh"]
