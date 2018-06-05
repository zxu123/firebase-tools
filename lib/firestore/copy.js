"use strict";

var api = require("../../lib/api");
var chalk = require("chalk");
var firebase = require("firebase");
var FirebaseError = require("../../lib/error");
var logger = require("../../lib/logger");
var ProgressBar = require("progress");

var utils = require("../../lib/utils");

/**
 * Construct a new Firestore copy operation.
 *
 * @constructor
 * @param {string} project the Firestore project ID.
 * @param {string} source path to a document or a collection.
 * @param {string} target path to target location.
 * @param {boolean} options.recursive true if copy should be recursive.
 * @param {boolean} options.shallow true if copy should be non-recursive.
 * @param {boolean} options.overwrite true if overwrite duplicate docs.
 * @param {boolean} options.skip true if skip duplicate docs.
 * @param {number} batchSize the number of documents to copy in a batch.
 */
function FirestoreCopy(project, source, target, options) {
  this.project = project;
  this.source = source;
  this.target = target;
  this.recursive = Boolean(options.recursive);
  this.shallow = Boolean(options.shallow);
  this.overwrite = Boolean(options.overwrite);
  this.skip = Boolean(options.skip);
  this.batchSize = options.batchSize || 50;
  this.docs = {};
  this.streamId = {};
  this.streamToken = {};

  // Remove any leading or trailing slashes from the path
  if (this.source) {
    this.source = this.source.replace(/(^\/+|\/+$)/g, "");
  }
  if (this.target) {
    this.target = this.target.replace(/(^\/+|\/+$)/g, "");
  }

  this.isDocumentPath = this._isDocumentPath(this.source);
  this.isCollectionPath = this._isCollectionPath(this.source);

  this.parent = "projects/" + project + "/databases/(default)/documents";

  this._validateOptions();
}

/**
 * Validate all options, throwing an exception for any fatal errors.
 */
FirestoreCopy.prototype._validateOptions = function() {
  if (this.recursive && this.shallow) {
    throw new FirebaseError("Cannot pass recursive and shallow options together.");
  }

  if (this.overwrite && this.skip) {
    throw new FirebaseError("Cannot pass overwrite and skip options together.");
  }

  this._validatePath(this.source);
  this._validatePath(this.target);
};

/**
 * Validate path, throwing an exception for any fatal errors.
 *
 * @param {string} path to a document or a collection.
 */
FirestoreCopy.prototype._validatePath = function(path) {
  var pieces = path.split("/");

  if (pieces.length === 0) {
    throw new FirebaseError("Path length must be greater than zero.");
  }

  var hasEmptySegment = pieces.some(function(piece) {
    return piece.length === 0;
  });

  if (hasEmptySegment) {
    throw new FirebaseError("Path must not have any empty segments.");
  }
};

/**
 * Determine if a path points to a document.
 *
 * @param {string} path a path to a Firestore document or collection.
 * @return {boolean} true if the path points to a document, false
 * if it points to a collection.
 */
FirestoreCopy.prototype._isDocumentPath = function(path) {
  if (!path) {
    return false;
  }

  var pieces = path.split("/");
  return pieces.length % 2 === 0;
};

/**
 * Determine if a path points to a collection.
 *
 * @param {string} path a path to a Firestore document or collection.
 * @return {boolean} true if the path points to a collection, false
 * if it points to a document.
 */
FirestoreCopy.prototype._isCollectionPath = function(path) {
  if (!path) {
    return false;
  }

  return !this._isDocumentPath(path);
};

/**
 * Get a single document.
 *
 * @param {string} source the source document location.
 * @return {Promise} a promise for getting a doc.
 */
// TODO(zxu): Really we should use batchGet. For now, use get to make this
// happen in a hackweek.
FirestoreCopy.prototype._getOneDocument = function(source) {
  var url = this.parent + "/" + source;
  var body = {};
  var self = this;
  return api
    .request("GET", "/v1beta1/" + url, {
      auth: true,
      data: body,
      origin: api.firestoreOrigin,
    })
    .then(function(res) {
      logger.debug("firestone:copy:get(" + source + ") status:" + res.status +
                   " fields:" + JSON.stringify(res.body.fields));
      self.docs[source] = res.body.fields;
    });
}

/**
 * Create a single document.
 *
 * @param {string} source the source document location.
 * @param {string} target the target document location.
 * @return {Promise} a promise for creating a doc.
 */
// TODO(zxu): Really we should use write. For now, use create to make this
// happen in a hackweek.
FirestoreCopy.prototype._createOneDocument = function(source, target) {
  var segments = target.split("/");
  var collectionPath = segments.slice(0, -1).join("/");
  var documentId = segments[segments.length - 1];
  var url = this.parent + "/" + collectionPath + "?documentId=" + documentId;
  var body = {
      "fields": this.docs[source],
  };
  var self = this;
  return api
    .request("POST", "/v1beta1/" + url, {
      auth: true,
      data: body,
      origin: api.firestoreOrigin,
    })
    .then(function(res) {
      logger.debug("firestone:copy:create(" + target + ") status:" + res.status +
                   " fields:" + JSON.stringify(res.body.fields));
    });
};

/**
 * Delete a single document.
 *
 * @param {string} target the target document location.
 * @return {Promise} a promise for deleting a doc.
 */
FirestoreCopy.prototype._deleteOneDocument = function(target) {
  var url = this.parent + "/" + target;
  var body = {};
  var self = this;
  return api
    .request("DELETE", "/v1beta1/" + url, {
      auth: true,
      data: body,
      origin: api.firestoreOrigin,
    })
    .then(function(res) {
      logger.debug("firestone:copy:delete(" + target + ") status:" + res.status);
      delete self.docs[target];
    });
}

/**
 * Copy a single document.
 *
 * @param {string} source the source document location.
 * @param {string} target the target document location.
 * @return {Promise} a promise for copy.
 */
FirestoreCopy.prototype._copyOneDocument = function(source, target) {
  var self = this;
  return self._getOneDocument(source)
    .then(function() { return self._getOneDocument(target); })
    .catch(function() {})  // There may be duplicate in the target.
    .then(function() {
      if (self.docs[target]) {
         if (self.overwrite) {
           logger.debug("overwrite duplicate doc " + target);
           return self._deleteOneDocument(target)
             .then(function() { return self._createOneDocument(source, target) });
         } else if (self.skip) {
           logger.debug("skip duplicate doc " + target);
           return;
         } else {
           // Should raise some warning to user here.
           logger.error("There is duplicate doc at " + target +
                        ". Please either specify --overwrite or --skip.");
           return;
         }
      } else {
         return self._createOneDocument(source, target);
      }
    });
};

/**
 * Copy a single collection.
 *
 * @param {string} source the source location.
 * @param {string} target the target location.
 * @return {Promise} a promise for copy.
 */
FirestoreCopy.prototype._copyOneCollection = function(source, target) {
  return;
};

/**
 * Copy all documents and collections recursively.
 *
 * @param {string} source the source location.
 * @param {string} target the target location.
 * @return {Promise} a promise for index creation.
 */
FirestoreCopy.prototype._copy = function(source, target) {
  return;
};

/**
 * Run the copy operation.
 */
FirestoreCopy.prototype.execute = function() {
  return this._copyOneDocument(this.source, this.target);
};

module.exports = FirestoreCopy;
