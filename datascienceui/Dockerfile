FROM node:latest
#FROM registry.ng.bluemix.net/ibmnode
ADD ./app /node-hello
ENV NODE_ENV production
EXPOSE 3000
RUN cd /node-hello; npm install
CMD cd /node-hello; npm start
