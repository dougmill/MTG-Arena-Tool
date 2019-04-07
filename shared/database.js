const electron = require("electron");
const fs = require('fs');

// Made a singleton class for this
// Makes it simpler to update ;)
// Some other things should go here later, like updating from MTGA Servers themselves.
class Database {
  constructor() {
    let json = JSON.parse(fs.readFileSync(`${__dirname}/../resources/database.json`, 'utf8'));
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
    return ret ? ret : false;
  }

  getByArt(artId) {
    let list = Object.keys(this.cards);
    let ret = list.filter(grpid => this.cards[grpid].artid == artId)[0];
    return ret ? this.cards[ret] : false;
  }

  getAbility(abId) {
    let ret = this.cards["abilities"][abId];
    return ret ? ret : "";
  }

  getAll() {
    let ret = this.cards;
    return ret;
  }
}

module.exports = Database;
