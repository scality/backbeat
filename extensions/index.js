const fs = require('fs');
const path = require('path');

const extensions = {};

fs.readdirSync(__dirname).forEach(moduleDir => {
    const extStat = fs.statSync(path.join(__dirname, moduleDir));
    if (extStat.isDirectory() && moduleDir !== 'utils') {
        let indexStat;
        try {
            indexStat = fs.statSync(
                path.join(__dirname, moduleDir, 'index.js'));
        } catch (err) {} // eslint-disable-line no-empty
        if (indexStat) {
            extensions[moduleDir] = require(`./${moduleDir}`);
        }
    }
});

module.exports = extensions;
