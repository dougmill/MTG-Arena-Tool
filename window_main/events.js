/*
globals
  ipc_send,
  matchesHistory,
  cardsDb,
  mana,
  get_rank_index_16,
  timeSince,
  toMMSS,
  get_deck_colors,
  addHover,
  compare_cards,
  getReadableEvent,
  createDivision,
  eventsHistory,
  compare_courses,
  loadEvents,
  currentId
*/

function openEventsTab(loadMore) {
  var mainDiv = document.getElementById("ux_0");
  if (loadMore <= 0) {
    loadMore = 25;
    eventsHistory.courses.sort(compare_courses);

    mainDiv.classList.remove("flex_item");
    mainDiv.innerHTML = "";

    var d = createDivision(["list_fill"]);
    mainDiv.appendChild(d);

    loadEvents = 0;
  }

  //console.log("Load more: ", loadEvents, loadMore, loadEvents+loadMore);
  for (
    var loadEnd = loadEvents + loadMore;
    loadEvents < loadEnd;
    loadEvents++
  ) {
    var course_id = eventsHistory.courses[loadEvents];
    var course = eventsHistory[course_id];

    if (course == undefined || course.CourseDeck == undefined) {
      continue;
    }

    var eventRow = createEventRow(course);
    var divExp = createDivision([course.id + "exp", "list_event_expand"]);

    mainDiv.appendChild(eventRow);
    mainDiv.appendChild(divExp);

    attachDeleteCourseButton(course);
    addHover(course, divExp);
  }

  $(this).off();
  $("#ux_0").on("scroll", function() {
    if (
      Math.round($(this).scrollTop() + $(this).innerHeight()) >=
      $(this)[0].scrollHeight
    ) {
      openEventsTab(20);
    }
  });

  $(".delete_item").hover(
    function() {
      // in
      $(this).css("width", "32px");
    },
    function() {
      // out
      $(this).css("width", "4px");
    }
  );

  loadEvents = loadEnd;
}

function createEventRow(course) {
  // create the DOM structure

  var eventContainer = createDivision([course.id, "list_match"]);

  var flexTopLeft = createDivision(["flex_item"]);

  var flexLeft = createDivision(["flex_item"]);
  flexLeft.style.flexDirection = "column";

  var flexTop = createDivision(["flex_top"]);
  flexLeft.appendChild(flexTop);

  var flexBottom = createDivision(["flex_bottom"]);
  flexLeft.appendChild(flexBottom);

  var flexCenter = createDivision(["flex_item"]);
  flexCenter.style.flexDirection = "column";
  flexCenter.style.flexGrow = 2;

  var flexCenterTop = createDivision(["flex_top"]);
  flexCenter.appendChild(flexCenterTop);

  var flexCenterBottom = createDivision(["flex_bottom"]);
  flexCenterBottom.style.marginRight = "14px";
  flexCenter.appendChild(flexCenterBottom);

  var flexRight = createDivision(["flex_item"]);

  var flexDeleteEvent = createDivision([
    "flex_item",
    course.id + "_del",
    "delete_item"
  ]);

  flexDeleteEvent.style.marginRight = "10px";

  eventContainer.appendChild(flexTopLeft);
  eventContainer.appendChild(flexLeft);
  eventContainer.appendChild(flexCenter);
  eventContainer.appendChild(flexRight);
  eventContainer.appendChild(flexDeleteEvent);

  var tileGrpid = course.CourseDeck.deckTileId;
  try {
    cardsDb.get(tileGrpid).set;
  } catch (e) {
    tileGrpid = 67003;
  }

  var tile = createDivision([course.id + "t", "deck_tile"]);

  try {
    tile.style.backgroundImage =
      "url(https://img.scryfall.com/cards" +
      cardsDb.get(tileGrpid).images["art_crop"] +
      ")";
  } catch (e) {
    console.error(e, tileGrpid);
  }
  flexTopLeft.appendChild(tile);

  flexTop.appendChild(
    createDivision(
      ["list_deck_name"],
      getReadableEvent(course.InternalEventName)
    )
  );

  course.CourseDeck.colors.forEach(color => {
    flexBottom.appendChild(createDivision(["mana_s20", `mana_${mana[color]}`]));
  });

  var eventState = course.CurrentEventState;
  if (eventState == "DoneWithMatches" || eventState == 2) {
    flexCenterTop.appendChild(
      createDivision(["list_event_phase"], "Completed")
    );
  } else {
    flexCenterTop.appendChild(
      createDivision(["list_event_phase_red"], "In progress")
    );
  }

  var duration = 0;

  var wlGate = course.ModuleInstanceData.WinLossGate;
  var matchesList = wlGate ? wlGate.ProcessedMatchIds : undefined;
  if (matchesList) {
    var matches = matchesList
      .map(index => matchesHistory[index])
      .filter(match => match !== undefined && match.type === "match");

    var duration = matches.reduce((acc, match) => acc + match.duration, 0);
  }

  flexCenterBottom.appendChild(
    createDivision(
      ["list_match_time"],
      timeSince(new Date(course.date)) + " ago - " + toMMSS(duration)
    )
  );

  var winLossText = "0:0";
  var winLossClass = "list_match_result_win";

  if (wlGate !== undefined) {
    winLossText = wlGate.CurrentWins + ":" + wlGate.CurrentLosses;
    if (wlGate.MaxWins !== wlGate.CurrentWins) {
      winLossClass = "list_match_result_loss";
    }
  }

  flexRight.appendChild(createDivision([winLossClass], winLossText));

  return eventContainer;
}

function attachDeleteCourseButton(course) {
  $(`.${course.id}_del`).on("click", function(e) {
    // This is a global. It's used in other parts of the code.
    currentId = course.id;
    e.stopPropagation();
    ipc_send("delete_course", currentId);
    $("." + currentId).css("height", "0px");
  });
}

// Given the data of a match will return a data row to be
// inserted into one of the screens.

// DOM should be consistent across all screens. Use CSS to style the row and hide elements.
//
// Used by events.js and potentially other screens.
function createMatchRow(match) {
  //  if (match.opponent == undefined) continue;
  //  if (match.opponent.userid.indexOf("Familiar") !== -1) continue;
  match.playerDeck.mainDeck.sort(compare_cards);
  match.oppDeck.mainDeck.sort(compare_cards);

  // Create the DOM structure
  // rowContainer
  //   flexTopLeft
  //   flexLeft
  //     flexTop
  //     flexBottom
  //   flexCenter
  //     flexCenterTop
  //     flexCenterBottom
  //   flexRight

  var rowContainer = createDivision([match.id, "list_match"]);

  var flexTopLeft = createDivision(["flex_item"]);

  var flexLeft = createDivision(["flex_item"]);
  flexLeft.style.flexDirection = "column";

  var flexTop = createDivision(["flex_top"]);
  flexLeft.appendChild(flexTop);

  var flexBottom = createDivision(["flex_bottom"]);
  flexLeft.appendChild(flexBottom);

  var flexCenter = createDivision(["flex_item"]);
  flexCenter.style.flexDirection = "column";
  flexCenter.style.flexGrow = 2;

  var flexCenterTop = createDivision(["flex_top"]);
  flexCenter.appendChild(flexCenterTop);

  var flexCenterBottom = createDivision(["flex_bottom"]);
  flexCenterBottom.style.marginRight = "14px";
  flexCenter.appendChild(flexCenterBottom);

  var flexRight = createDivision(["flex_item"]);

  rowContainer.appendChild(flexTopLeft);
  rowContainer.appendChild(flexLeft);
  rowContainer.appendChild(flexCenter);
  rowContainer.appendChild(flexRight);

  // Insert contents of flexTopLeft

  var tileGrpid = match.playerDeck.deckTileId;
  try {
    cardsDb.get(tileGrpid).images["art_crop"];
  } catch (e) {
    tileGrpid = 67003;
  }

  var tile = createDivision([match.id + "t", "deck_tile"]);
  try {
    let backgroundFile = cardsDb.get(tileGrpid).images["art_crop"];
    tile.style.backgroundImage = `url(https://img.scryfall.com/cards${backgroundFile})`;
  } catch (e) {
    timeSince(new Date(match.date)) + " ago - " + toMMSS(match.duration);
    console.error(e);
  }
  flexTopLeft.appendChild(tile);

  // Insert contents of flexTop
  flexTop.appendChild(
    createDivision(["list_deck_name"], match.playerDeck.name)
  );

  // Insert contents of flexBottom
  match.playerDeck.colors.forEach(function(color) {
    var m = createDivision(["mana_s20", "mana_" + mana[color]]);
    flexBottom.appendChild(m);
  });

  // Insert contents of flexCenterTop
  if (match.opponent.name == null) {
    match.opponent.name = "-";
  }

  flexCenterTop.appendChild(
    createDivision(
      ["list_match_title"],
      "vs " + match.opponent.name.slice(0, -6)
    )
  );

  var or = createDivision(["ranks_16"]);
  or.style.backgroundPosition = `${get_rank_index_16(match.opponent.rank) *
    -16}px 0px`;
  or.title = match.opponent.rank + " " + match.opponent.tier;
  flexCenterTop.appendChild(or);

  // insert contents of flexCenterBottom
  flexCenterBottom.appendChild(
    createDivision(
      ["list_match_time"],
      timeSince(new Date(match.date)) + " ago - " + toMMSS(match.duration)
    )
  );

  get_deck_colors(match.oppDeck).forEach(function(color) {
    var m = createDivision(["mana_s20", "mana_" + mana[color]]);
    flexCenterBottom.appendChild(m);
  });

  // insert contents of flexRight

  var resultClass = `list_match_result_${
    match.player.win > match.opponent.win ? "win" : "loss"
  }`;
  flexRight.appendChild(
    createDivision([resultClass], match.player.win + ":" + match.opponent.win)
  );

  return rowContainer;
}

// This code is executed when an event row is clicked and adds
// rows below the event for every match in that event.
function expandEvent(course, expandDiv) {
  if (expandDiv.hasAttribute("style")) {
    expandDiv.removeAttribute("style");
    setTimeout(function() {
      expandDiv.innerHTML = "";
    }, 200);
    return;
  }

  var matchesList = course.ModuleInstanceData.WinLossGate.ProcessedMatchIds;
  expandDiv.innerHTML = "";

  if (!matchesList) {
    return;
  }

  var matchRows = matchesList
    .map(index => matchesHistory[index])
    .filter(match => match !== undefined && match.type === "match")
    .map(match => {
      let row = createMatchRow(match);
      expandDiv.appendChild(row);
      addHover(match, expandDiv);
      return row;
    });

  var newHeight = matchRows.length * 64 + 16;

  expandDiv.style.height = `${newHeight}px`;
}

module.exports = {
  openEventsTab: openEventsTab,
  expandEvent: expandEvent
};
