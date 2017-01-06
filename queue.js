var uuid = require('uuid');

var pushQuery = 'INSERT into jobs (queue, payload, created_at, available_at) VALUES (?, ?, ?, ?)';
var popQuery = 'SELECT * FROM jobs WHERE reserved_by = ? LIMIT 1';
var reserveQuery = 'UPDATE jobs SET reserved = 1, reserved_at = ?, reserved_by = ? WHERE reserved = 0 AND queue = ? AND available_at <= ? LIMIT 1';
var deleteQuery = 'DELETE FROM jobs WHERE id = ?';

function Queue(client, queueName) {
    this.client = client;
    this.queueName = queueName;
}

Queue.prototype.push = function push(data, callback) {
    var self = this;
    var queueData = createPayload(data);
    var now = getTime();
    var params = [self.queueName, queueData, now, now];
    return insertMethod(self.client, pushQuery, params, callback);
};

Queue.prototype.pushLater = function pushLater(data, delay, callback) {
    var self = this;
    var queueData = createPayload(data);
    var now = getTime();
    var available = now + delay;

    var params = [self.queueName, queueData, now, available];
    return insertMethod(self.client, pushQuery, params, callback);
}

Queue.prototype.pop = function (queue, callback) {
    var self = this;
    var uniqIdentifier = uuid.v4();
    var now = getTime();

    self.client.query(reserveQuery, [now, uniqIdentifier, queue, now], reserveJob);

    function reserveJob(err, result) {
        if (err) {
            return callback(err);
        }
        if (result.affectedRows > 0) {
            self.client.query(popQuery, [uniqIdentifier], getNextAvailableJob);
        } else {
            return callback(null, null, 'No job');
        }
    }

    function getNextAvailableJob(err, result) {
        if (result[0].length == 0) {
            return callback(null, null, 'No job');
        } else {
            return callback(err, result[0].id, decodePayload(result[0].payload));
        }
    }
}
Queue.prototype.destroy = function destroy(jobId, callback) {
    var self = this;

    self.client.query(deleteQuery, [jobId], function (err, result) {
        if (err) console.error('delete error:', err);
        return callback(err, result);
    });
};

Queue.prototype.setQueue = function setQueue(queueName) {
    this.queueName = queueName;
    return;
};

function insertMethod(client, query, params, callback) {
    client.query(query, params, function (err, result) {
        if (err) {
            return callback(err, null);
        } else {
            return callback(null, result.insertId);
        }
    });
}

function createPayload(data) {
    return JSON.stringify(data);
}

function decodePayload(data) {
    return JSON.parse(data);
}

function getTime() {
    return Math.floor(Date.now() / 1000);
}

module.exports = Queue;
