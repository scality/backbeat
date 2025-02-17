const fs = require('fs');
const path = require('path');

const extensions = {};

fs.readdirSync(__dirname).forEach(moduleDir => {
    const indexPath = path.join(__dirname, moduleDir, 'index.js');
    
    if (fs.existsSync(indexPath)) {
        extensions[moduleDir] = require(`./${moduleDir}`);
    }
});

module.exports = extensions;
