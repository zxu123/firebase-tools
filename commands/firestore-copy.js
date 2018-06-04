"use strict";

var chalk = require("chalk");
var Command = require("../lib/command");
var FirestoreCopy = require("../lib/firestore/copy");
var prompt = require("../lib/prompt");
var requireAccess = require("../lib/requireAccess");

var scopes = require("../lib/scopes");
var utils = require("../lib/utils");

module.exports = new Command("firestore:copy [source] [target]")
  .description("Copy Firestore documents and collections from one location to another.")
  .option(
    "-r, --recursive",
    "Recursive. Copy all documents and subcollections. " +
      "Any action which would result in the deletion of child documents will fail if " +
      "this argument is not passed. May not be passed along with --shallow."
  )
  .option(
    "--shallow",
    "Shallow. Copy only parent documents and ignore documents in subcollections. " +
      "May not be passed along with -r."
  )
  .option(
    "--overwrite",
    "Overwrite. When there is a duplicate document in the target location, overwrite it. " +
      "May not be passed along with --skip."
  )
  .option(
    "--skip",
    "Skip. When there is a duplicate document in the target location, skip copying it. " +
      "May not be passed along with --overwrite."
  )
  .option("-y, --yes", "No confirmation. Otherwise, a confirmation prompt will appear.")
  .before(requireAccess, [scopes.CLOUD_PLATFORM])
  .action(function(source, target, options) {
    // Guarantee path
    if (!source || !target) {
      return utils.reject("Must specify a source path and a target path.", { exit: 1 });
    }

    var copyOp = new FirestoreCopy(options.project, source, target, {
      recursive: options.recursive,
      shallow: options.shallow,
      overwrite: options.overwrite,
      skip: options.skip,
      batchSize: 50,
    });

    return copyOp.execute();
  });

