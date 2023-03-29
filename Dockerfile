FROM node:18.15.0
WORKDIR /app
COPY package.json package-lock.json /app/
RUN npm install
RUN npm install moment
RUN npm install faker pg
COPY . /app/

CMD ["npm", "start"]