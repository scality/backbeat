function getLocationsFromStorageClass(replicationStorageClass) {
    return replicationStorageClass.split(',').map(s => {
        if (s.endsWith(':preferred_read')) {
            return s.split(':')[0];
        }
        return s;
    });
}

module.exports = getLocationsFromStorageClass;
