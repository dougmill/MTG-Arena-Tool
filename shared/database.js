const electron = require("electron");
const json = require("./database.json");

// Made a singleton class for this
// Makes it simpler to update ;)
// Some other things should go here later, like updating from MTGA Servers themselves.
class Database {
  constructor() {
    this.cards = json;
  }

  set(arg) {
    try {
      this.cards = JSON.parse(arg);
    } catch (e) {
      this.cards = arg;
    }

    return true;
  }

  get(grpId) {
    let ret = this.cards[grpId];
    if (ret == undefined) {
      //console.error("card not found: "+grpId);
      return false;
    }
    return ret;
  }

  getAbility(abId) {
    let ret = this.cards["abilities"][abId];
    if (ret == undefined) {
      return "";
    }
    return ret;
  }

  getAll() {
    let ret = this.cards;
    return ret;
  }
}

module.exports = Database;
