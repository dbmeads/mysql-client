const mysql = require('mysql');

/**
 *
 * @param options
 *  - host
 *  - port
 *  - user
 *  - password
 *  - db
 *  - tables
 * @returns {db}
 * @constructor
 */
function MySQL(options) {
    let connection = mysql.createConnection(options);
    let idField = options.idField || 'id';

    if (options.debug) {
        console.log(`SQL Options: ${JSON.stringify(options)}`);
    }

    function query(statement, values) {
        return new Promise((resolve, reject) => {
            if (options.debug) {
                console.log(`Statement: ${statement}\nValues: ${values}`);
            }
            connection.query(statement, values || [], (err, rows, fields) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows, fields);
                }
            });
        });
    }

    function db(table) {
        function find(filter) {
            let statement = `SELECT * FROM ${table}`;
            let values = [];
            if (filter && Object.keys(filter).length > 0) {
                statement += ' WHERE ';
                let len = statement.length;
                Object.keys(filter).forEach(column => {
                    statement += `${statement.length > len ? ' AND ' : ''}${column} = ?`;
                    values.push(filter[column]);
                });
            }
            return query(statement, values);
        }

        function findOne(filter) {
            return find(filter).then(rows => rows[0]);
        }

        function insert(obj) {
            let columns = '';
            let markers = '';
            let values = [];
            Object.keys(obj).forEach(column => {
                if (columns.length > 0) {
                    columns += `, ${column}`;
                    markers += ',?';
                } else {
                    columns += `${column}`;
                    markers += '?';
                }
                values.push(obj[column]);
            });
            return query(`INSERT INTO ${table} (${columns}) VALUES (${markers})`, values)
                .then(result => result.insertId);
        }

        function update(obj) {
            let statement = `UPDATE ${table} SET `;
            let len = statement.length;
            let values = [];
            Object.keys(obj).forEach(column => {
                if (column != idField) {
                    statement += `${statement.length > len ? ',' : ''}${column} = ?`;
                    values.push(obj[column]);
                }
            });
            values.push(obj[idField]);
            return query(statement + ' WHERE ID = ?', values)
                .then(() => obj[idField]);
        }

        function save(obj) {
            return obj[idField] ? update(obj) : insert(obj);
        }

        function truncate() {
            return query(`TRUNCATE ${table}`);
        }

        return {
            find,
            findOne,
            insert,
            save,
            truncate,
            update
        };
    };

    db.query = query;

    if (options.tables) {
        db.tables = {};
        for (let table of options.tables) {
            db.tables[table] = db(table);
        }
    }

    return db;
}

module.exports = MySQL;