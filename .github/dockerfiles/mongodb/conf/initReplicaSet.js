/* eslint-disable no-undef */
// the member host is what clients following the replica set configuration
// connect to: it must resolve from where they run
rs.initiate({
    _id: 'rs0',
    members: [{ _id: 0, host: process.env.MONGO_RS_HOST || '127.0.0.1:27018' }]
});
