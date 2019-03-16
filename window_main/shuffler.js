/*
globals
	add,
	addCardSeparator,
	add_checkbox,
	compareNumbers,
	cumulativeBinomial,
	hasValue,
	hypergeometric,
	hypergeometricDistribution,
	ipc_send,
	makeId,
	selectAdd
*/

const Chart = require("chart.js");
Chart.defaults.global.defaultFontColor = "#FAE5D2";
Chart.defaults.global.defaultFontFamily = "roboto";

const ChartDataLabels = require("chartjs-plugin-datalabels");

/*
	All of these share an inner structure of the following form:
	{
		smoothed: <same structure as standard>,
		smoothedGames: number of games that used smoothed shuffling,
		standard: [ // outer index is size of deck or library (sparse array)
			[ // second index is number of relevant cards in deck or library (sparse array)
				{
					numGames: number,
					chance: <non-rigorous probability assessment for the overall set of extrapolated data>
					known: <same structure as extrapolated>
					extrapolated: [ // outer index is 0-based part of a sequence, either number of cards or number of mulligans
						{
							chance: <non-rigorous probability assessment for the overall inner distribution>,
							counts: [ // second index is how many relevant cards were in the group of cards corresponding to the outer index
								{
									chance: <rigorous probability of this count or farther from average, considered alone>,
									count: <number of games>
								}
							]
						}
					]
				}
			]
		]
		standardGames: number of games that used standard shuffling
	}

	They have additional wrapping around this structure as follows:
	landStats: {
		... (no wrapping)
	}
	deckLandStats: [ // outer index is size of deck (sparse array)
		[ // second index is number of lands in deck (sparse array)
			{
				<"1", "3", or "all", stringified bestOf>: {
					...
				}
			}
		]
	]
	handStats: {
		<"1", "3", or "all", stringified bestOf>: {
			... (extrapolated is omitted)
		}
	}
	cardStats: {
		<"first" or "all", for whether only the first card with the right number of
		    copies in the decklist is counted>: {
			<"1", "3", or "all", stringified bestOf>: {
				...
			}
		}
	}
 */
let landStats = null;
let deckLandStats = null;
let handStats = null;
let cardStats = null;

let latestRequests = {};
let latestUpdate = "";
let latestRefresh = "";
let refreshDiv = null;
let refreshButton = null;
let refreshRecheckScheduled = false;

let currentQuery = {};
let searchParamsDiv = null;
let landsTableDiv = null;
let handsTableDiv = null;
let cardsTableDiv = null;

let currentLandsView = {
  viewing: "table",
  showBarNumbers: false,
  showGameNumbers: false,
  shuffling: "standard",
  extrapolated: false,
  searchAll: true
};
let currentHandsView = {
  viewing: "table",
  showBarNumbers: false,
  showGameNumbers: false,
  shuffling: "standard",
  bestOf: "all"
};
let currentCardsView = {
  viewing: "table",
  showBarNumbers: false,
  showGameNumbers: false,
  shuffling: "standard",
  sets: "first",
  extrapolated: false,
  bestOf: "all"
};

function requestData(query) {
  let lastRequestedAt = query
    ? pathToVal(latestRequests, query.deckSize, query.landsInDeck, query.bestOf)
    : latestRequests.date;
  if (lastRequestedAt && new Date() - lastRequestedAt < 20000) return;
  ipc_send("request_shuffler", query ? query : {});
  if (query) {
    setValToPath(
      latestRequests,
      new Date(),
      query.deckSize,
      query.landsInDeck,
      query.bestOf
    );
  } else {
    latestRequests.date = new Date();
  }
}

function receiveStats(statsString) {
  let stats = JSON.parse(statsString, (k, v) => {
    // filter out null values, but can't use Array.filter because that changes
    // the indexes
    if (Array.isArray(v)) {
      v.forEach((e, i) => {
        if (e === null) {
          delete v[i];
        }
      });
    }
    return v;
  });

  if (stats.landStats) {
    ({ landStats, handStats, cardStats } = stats);
    computeCombinedBestOf(handStats, true);
    computeCombinedBestOf(cardStats.first, false);
    computeCombinedBestOf(cardStats.all, false);
    latestUpdate = [landStats.date, handStats.date, cardStats.date].reduce(
      (a, b) => (a > b ? a : b)
    );
  } else {
    // Deck size/land specific stats are fetched individually, so merge the
    // results rather than replacing wholesale.
    if (!deckLandStats) deckLandStats = [];
    stats.deckLandStats.forEach((outer, deckSize) => {
      if (!deckLandStats[deckSize]) deckLandStats[deckSize] = [];
      outer.forEach((inner, landsInDeck) => {
        if (!deckLandStats[deckSize][landsInDeck]) {
          deckLandStats[deckSize][landsInDeck] = {};
        }
        let bestOfStats = deckLandStats[deckSize][landsInDeck];
        Object.assign(bestOfStats, inner);
        computeCombinedBestOf(bestOfStats, false);
      });
    });
  }

  updateRefreshButton();

  updateLandsTable();
  updateHandsTable();
  updateCardsTable();
}

function computeCombinedBestOf(bestOf, isHands) {
  bestOf["all"] = combineBestOf(bestOf["1"], bestOf["3"], isHands);
}

// Recursive merge of the common inner structure listed for the stats
// collections, to create a copy that includes both Bo1 and Bo3 games.
function combineBestOf(one, three, isHands, size, hits, sample) {
  // If we only have one or the other, no need to merge. These objects are never
  // altered, either, so can just return by reference.
  if (!(one && three)) {
    return one || three;
  }

  if (Array.isArray(one)) {
    let all = [];
    one.forEach((e, i) => {
      // No array in the structure directly includes a number, so it's either
      // copy reference or create a merge. Put the array index in the first
      // still-empty param that corresponds to an index.
      if (three[i]) {
        all[i] = combineBestOf(
          e,
          three[i],
          isHands,
          size || i,
          hits || (size && i),
          // For hands, the third index is the number of mulligans. For anything
          // else, 0-based position in the deck/library.
          sample || (size && hits && (isHands ? 7 - i : i + 1))
        );
      } else {
        all[i] = e;
      }
    });
    three.forEach((e, i) => {
      if (!one[i]) {
        all[i] = e;
      }
    });
    return all;
  }

  let all = {};
  ["smoothed", "standard"].forEach(shuffling => {
    let gamesKey = shuffling + "Games";
    if (one[gamesKey] && three[gamesKey]) {
      all[shuffling] = combineBestOf(
        one[shuffling],
        three[shuffling],
        isHands,
        size,
        hits,
        sample
      );
      all[gamesKey] = one[gamesKey] + three[gamesKey];
    } else if (one[gamesKey] || three[gamesKey]) {
      all[shuffling] = one[gamesKey] ? one[shuffling] : three[shuffling];
      all[gamesKey] = one[gamesKey] || three[gamesKey];
    }
  });
  if (all.smoothed || all.standard) return all;

  function combineKeys(...keys) {
    keys.forEach(key => {
      // Some of these have numerical values that may be 0, and 0 should not be
      // treated as missing.
      if (hasValue(one[key]) && hasValue(three[key])) {
        all[key] =
          typeof one[key] === "number"
            ? one[key] + three[key]
            : combineBestOf(one[key], three[key], isHands, size, hits, sample);
      } else if (hasValue(one[key]) || hasValue(three[key])) {
        all[key] = hasValue(one[key]) ? one[key] : three[key];
      }
    });
  }
  combineKeys("numGames", "known", "extrapolated", "counts", "count");

  // Now to recalculate the probability numbers, as those can't be simply
  // combined. counts is the innermost array, so start there.
  if (all.counts) {
    let numGames;
    numGames = Math.round(all.counts.map(e => e.count).reduce(add));
    if (numGames === 0) {
      all.counts.forEach(e => (e.chance = null));
    } else {
      let expectedDistribution = hypergeometricDistribution(size, sample, hits);
      all.counts.forEach((e, i) => {
        let expectedCount = numGames * (expectedDistribution[i] || 0);
        if (expectedCount === 0 || expectedCount === numGames) {
          e.chance = null;
          return;
        }
        // In case of extrapolation, don't let an above-average point early on
        // get extrapolated into an above-average fringe case resulting in a
        // spurious near-0 probability number. Leave the bounds unrounded,
        // however, to take advantage of the implementation of
        // cumulativeBinomial which is generalized to accept non-integer ranges.
        // Double the returned result to expand the above-average and
        // below-average ranges back up to 0 to 1, producing a
        // proximity-to-average assessment.
        let useBottom = e.count < expectedCount || e.count < 0.5;
        let lowerBound = useBottom ? 0 : e.count;
        let upperBound = useBottom ? e.count : numGames;
        e.chance = Math.min(
          cumulativeBinomial(
            numGames,
            lowerBound,
            upperBound,
            expectedDistribution[i]
          ) * 2,
          1
        );
      });
    }
    all.chance = combineProbabilitiesFromArray(all.counts);
  }

  // Base the assessment on extrapolation to prevent skew from games ending
  // early, but limit to the first 10 cards where extrapolation doesn't have
  // room to overwhelm actual data. Hand stats don't have extrapolation, so for
  // them fall back on the known numbers.
  if (all.extrapolated) {
    all.chance = combineProbabilitiesFromArray(all.extrapolated.slice(0, 10));
  } else if (all.known) {
    all.chance = combineProbabilitiesFromArray(all.known);
  }
  return all;
}

function combineProbabilitiesFromArray(arr) {
  let chance = null;
  arr.forEach(e => {
    if (e.chance === null) return;
    chance =
      chance !== null ? combineProbabilities(chance, e.chance) : e.chance;
  });
  return chance;
}

// Multiply to preserve the impact of excessively low probability events, rather
// than taking the average - if one probability is a 1-in-a-billion outlier,
// that should not be hidden by the other being 80%. Transform back to a uniform
// distribution with a formula taken from
// https://math.stackexchange.com/questions/659254/product-distribution-of-two-uniform-distribution-what-about-3-or-more
function combineProbabilities(a, b) {
  let c = a * b;
  return c === 0 ? 0 : c - c * Math.log(c);
}

function open_shuffler_tab() {
  let mainDiv = $("#ux_0");
  mainDiv.html("");
  mainDiv.removeClass("flex_item");
  let shufflerDiv = $('<div class="shuffler_stats"></div>');
  shufflerDiv.appendTo(mainDiv);
  shufflerDiv.append(
    $('<div class="section_header">Library Lands Statistics</div>')
  );
  addLandsDiv(shufflerDiv);
  shufflerDiv.append(
    $('<div class="section_header">Hand Lands Statistics</div>')
  );
  addHandsDiv(shufflerDiv);
  shufflerDiv.append(
    $('<div class="section_header">Deck Cards Statistics</div>')
  );
  addCardsDiv(shufflerDiv);
}

function addLandsDiv(shufflerDiv) {
  let landsDiv = $('<div class="shuffler_stats"></div>');
  refreshDiv = $('<div class="stats_params"></div>');
  refreshButton = $('<div class="button_simple_disabled">Refresh Data</div>');
  refreshButton.appendTo(refreshDiv);
  updateRefreshButton();
  refreshDiv.appendTo(landsDiv);
  searchParamsDiv = $('<div class="shuffler_stats"></div>');
  updateQuerySelects();

  landsTableDiv = $('<div class="shuffler_table"></div>');
  updateLandsTable();

  searchParamsDiv.appendTo(landsDiv);
  landsTableDiv.appendTo(landsDiv);
  landsDiv.appendTo(shufflerDiv);
}

function addHandsDiv(shufflerDiv) {
  let paramsDiv = $('<div class="stats_params"></div>');
  addLabeledQuerySelect(
    paramsDiv,
    "Best of:",
    currentHandsView,
    "bestOf",
    ["1", "3", "all"],
    updateHandsTable
  );
  addLabeledQuerySelect(
    paramsDiv,
    "Shuffling:",
    currentHandsView,
    "shuffling",
    ["standard", "smoothed"],
    updateHandsTable
  );
  paramsDiv.appendTo(shufflerDiv);
  handsTableDiv = $('<div class="shuffler_table"></div>');
  updateHandsTable();
  handsTableDiv.appendTo(shufflerDiv);
}

function addCardsDiv(shufflerDiv) {
  let paramsDiv = $('<div class="stats_params"></div>');
  addQueryCheckbox(
    paramsDiv,
    "Include extrapolations",
    currentCardsView,
    "extrapolated",
    updateCardsTable
  );
  let cardsetsLabelDiv = $(
    '<div class="stats_params"><label class="stats_param_label" ' +
      'tooltip-bottom tooltip-content="The positions of cards from two ' +
      "different sets in the same game are not independent, and this affects " +
      "the probabilities in ways difficult to calculate. Considering only one " +
      "set of cards from each game allows accurate statistical calculations. " +
      'If you select to use all, the chance numbers may be inaccurate.">' +
      "Cardsets per Game:</label></div>"
  );
  cardsetsLabelDiv.appendTo(paramsDiv);
  addQuerySelect(
    cardsetsLabelDiv,
    currentCardsView,
    "sets",
    ["first", "all"],
    updateCardsTable
  );
  addLabeledQuerySelect(
    paramsDiv,
    "Best of:",
    currentCardsView,
    "bestOf",
    ["1", "3", "all"],
    updateCardsTable
  );
  addLabeledQuerySelect(
    paramsDiv,
    "Shuffling:",
    currentCardsView,
    "shuffling",
    ["standard", "smoothed"],
    updateCardsTable
  );
  paramsDiv.appendTo(shufflerDiv);
  cardsTableDiv = $('<div class="shuffler_table"></div>');
  updateCardsTable();
  cardsTableDiv.appendTo(shufflerDiv);
}

function updateRefreshButton() {
  if (!latestRefresh) latestRefresh = new Date().toJSON();

  // Wait for the most recent game included to be 40 minutes old, and for 10
  // minutes since the last time refresh was attempted.
  let updateTimesOut = latestUpdate
    ? new Date(latestUpdate).getTime() + 2400000
    : 0;
  let clickTimesOut = new Date(latestRefresh).getTime() + 600000;
  let nextRefresh = Math.max(updateTimesOut, clickTimesOut);
  let now = Date.now();
  let timeUntilRefresh = nextRefresh - now;

  if (timeUntilRefresh <= 0) {
    refreshButton.detach();
    refreshDiv.html("");
    refreshButton.css("margin-left", "auto");
    refreshDiv.append(refreshButton);
    if (refreshButton.hasClass("button_simple_disabled")) {
      refreshButton.removeClass("button_simple_disabled");
      refreshButton.addClass("button_simple");
      refreshButton.click(() => {
        landStats = null;
        deckLandStats = null;
        handStats = null;
        cardStats = null;
        latestRefresh = new Date().toJSON();
        updateRefreshButton();
        updateLandsTable();
        updateHandsTable();
        updateCardsTable();
        if (!currentLandsView.searchAll) {
          requestData();
        }
      });
    }
  } else {
    refreshButton.detach();
    refreshDiv.html("");
    refreshButton.css("margin-left", 0);
    let formatOptions = {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit"
    };
    let refreshText = `Can refresh at: ${new Date(nextRefresh).toLocaleString(
      "en-US",
      formatOptions
    )}`;
    refreshDiv.append(
      `<label class="stats_param_label" style="margin-left: auto">${refreshText}</label>`
    );
    refreshDiv.append(refreshButton);

    refreshButton.removeClass("button_simple");
    refreshButton.addClass("button_simple_disabled");
    refreshButton.off();
    // To prevent a bunch of scheduled rechecks from piling up, only schedule
    // one if there is not one currently scheduled.
    if (!refreshRecheckScheduled) {
      refreshRecheckScheduled = true;
      setTimeout(() => {
        refreshRecheckScheduled = false;
        updateRefreshButton();
      }, timeUntilRefresh);
    }
  }
}

function updateQuerySelects() {
  searchParamsDiv.html("");
  let toggles = $('<div class="stats_params"></div>');
  addQueryCheckbox(
    toggles,
    "Include extrapolations",
    currentLandsView,
    "extrapolated",
    updateLandsTable
  );
  addQueryCheckbox(
    toggles,
    "Search all decks",
    currentLandsView,
    "searchAll",
    updateQuerySelects,
    updateLandsTable
  );
  addLabeledQuerySelect(
    toggles,
    "Shuffling:",
    currentLandsView,
    "shuffling",
    ["standard", "smoothed"],
    updateLandsTable
  );
  toggles.appendTo(searchParamsDiv);
  if (!currentLandsView.searchAll) {
    let selections = $('<div class="stats_params"></div>');
    selections.appendTo(searchParamsDiv);
    addLabeledQuerySelect(
      selections,
      "Deck Size:",
      currentQuery,
      "deckSize",
      [40, 41, 42, 60, 61, 62],
      updateQuerySelects,
      updateLandsTable
    );
    let landOptions =
      currentQuery.deckSize < 60
        ? [14, 15, 16, 17, 18, 19]
        : [18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28];
    if (
      currentQuery.landsInDeck &&
      !landOptions.includes(currentQuery.landsInDeck)
    ) {
      delete currentQuery.landsInDeck;
    }
    addLabeledQuerySelect(
      selections,
      "Lands in Deck:",
      currentQuery,
      "landsInDeck",
      landOptions,
      updateQuerySelects,
      updateLandsTable
    );
    addLabeledQuerySelect(
      selections,
      "Best of:",
      currentQuery,
      "bestOf",
      ["1", "3", "all"],
      updateQuerySelects,
      updateLandsTable
    );
  }
}

function addQueryCheckbox(
  div,
  label,
  valueObject,
  valueField,
  ...updateListeners
) {
  function onUpdate() {
    valueObject[valueField] = this.checked;
    updateListeners.forEach(func => func());
  }
  add_checkbox(div, label, makeId(6), valueObject[valueField], onUpdate);
}

function addLabeledQuerySelect(
  div,
  label,
  valueObject,
  valueField,
  values,
  ...updateListeners
) {
  let labelDiv = $(
    `<div class="stats_params"><label class="stats_param_label">${label}</label></div>`
  );
  labelDiv.appendTo(div);
  addQuerySelect(labelDiv, valueObject, valueField, values, ...updateListeners);
}

function addQuerySelect(
  div,
  valueObject,
  valueField,
  values,
  ...updateListeners
) {
  let select = $("<select></select>");
  values.forEach(value =>
    select.append(
      $(`<option value="${value}">${String(value).capitalize()}</option>`)
    )
  );
  select.appendTo(div);
  function onUpdate(value) {
    valueObject[valueField] = typeof values[0] === "string" ? value : +value;
    updateListeners.forEach(func => func());
  }
  selectAdd(select, onUpdate, String(values[0]).length > 3 ? 130 : 100);
  select
    .next("div.select-styled")
    .text(String(valueObject[valueField] || "").capitalize());
}

function updateLandsTable() {
  if (
    !currentLandsView.searchAll &&
    !(currentQuery.deckSize && currentQuery.landsInDeck && currentQuery.bestOf)
  ) {
    document.body.style.cursor = "auto";
    let prompt = $(
      '<div class="prompt">Must select values for all three parameters</div>'
    );
    prompt.css("grid-row-end", "span 2");
    landsTableDiv.html("");
    prompt.appendTo(landsTableDiv);
    return;
  }

  if (currentLandsView.searchAll) {
    updateTable({
      div: landsTableDiv,
      viewTracker: currentLandsView,
      columnsHeader: "Cards in Library",
      rowsHeader: "Lands in Library",
      stats: landStats,
      chartSelectorLabelFunc: index => `Top ${index + 1} Cards`,
      chartTitleFunc: (lands, cards) =>
        `${lands} Lands out of ${cards} Cards in Library`,
      chartAxisLabelFunc: chartIndex =>
        `Lands in the top ${chartIndex + 1} cards`
    });
    if (!landStats) {
      requestData();
      let prompt = $(
        '<div class="prompt">Loading... This may take up to 30 seconds.</div>'
      );
      prompt.css("grid-row-end", "span 2");
      prompt.appendTo(landsTableDiv);
    }
  } else {
    let bestOfStats = pathToVal(
      deckLandStats,
      currentQuery.deckSize,
      currentQuery.landsInDeck
    );
    let tableParams = {
      div: landsTableDiv,
      viewTracker: currentLandsView,
      columnsHeader: "Cards in Library",
      rowsHeader: "Lands in Library",
      stats: pathToVal(bestOfStats, currentQuery.bestOf),
      chartSelectorLabelFunc: index => `Top ${index + 1} Cards`,
      chartTitleFunc: (lands, cards) =>
        `${lands} Lands out of ${cards} Cards in Library`,
      chartAxisLabelFunc: chartIndex =>
        `Lands in the top ${chartIndex + 1} cards`
    };
    let bestOfValues =
      currentQuery.bestOf === "all" ? ["1", "3"] : [currentQuery.bestOf];
    let needDataFor = new Set(bestOfValues);
    bestOfValues.forEach(bestOf => {
      if (bestOfStats && bestOfStats[bestOf]) {
        needDataFor.delete(bestOf);
      } else {
        requestData({ ...currentQuery, bestOf: bestOf });
      }
    });
    updateTable(tableParams);
  }
}

function updateHandsTable() {
  updateTable({
    div: handsTableDiv,
    viewTracker: currentHandsView,
    columnsHeader: "Cards in Deck",
    rowsHeader: "Lands in Deck",
    stats: pathToVal(handStats, currentHandsView.bestOf),
    chartSelectorLabelFunc: index => `${index} Mulligans`,
    chartTitleFunc: (lands, cards) =>
      `${lands} Lands out of ${cards} Cards in Deck`,
    chartAxisLabelFunc: chartIndex =>
      `Lands in opening hand after ${chartIndex} mulligans`
  });
}

function updateCardsTable() {
  updateTable({
    div: cardsTableDiv,
    viewTracker: currentCardsView,
    columnsHeader: "Cards in Deck",
    rowsHeader: "Card Copies in Deck",
    stats: pathToVal(cardStats, currentCardsView.sets, currentCardsView.bestOf),
    chartSelectorLabelFunc: index => `Top ${index + 1} Cards`,
    chartTitleFunc: (copies, cards) =>
      `${copies} Copies out of ${cards} Cards in Deck`,
    chartAxisLabelFunc: chartIndex =>
      `Copies in the top ${chartIndex + 1} cards`,
    unitLabel: "cardsets"
  });
}

function pathToVal(source, ...path) {
  return path.reduce((acc, p) => (acc ? acc[p] : acc), source);
}

function setValToPath(dest, val, ...path) {
  path.forEach((p, i) => {
    if (i === path.length - 1) {
      dest[p] = val;
    } else if (dest[p] !== null && typeof dest[p] === "object") {
      dest = dest[p];
    } else {
      dest[p] = typeof path[i + 1] === "number" ? [] : {};
    }
  });
}

function updateTable(params) {
  let {
    div,
    viewTracker,
    columnsHeader,
    rowsHeader,
    stats,
    chartSelectorLabelFunc,
    chartTitleFunc,
    chartAxisLabelFunc,
    unitLabel = "games"
  } = params;

  let tableStats = pathToVal(stats, viewTracker.shuffling);
  if (!tableStats) {
    div.html(
      '<div class="loading_bar ux_loading"><div class="loading_color loading_w"></div><div class="loading_color loading_u"></div><div class="loading_color loading_b"></div><div class="loading_color loading_r"></div><div class="loading_color loading_g"></div></div>'
    );
    document.body.style.cursor = "progress";
    return;
  }

  let totalGames = stats[viewTracker.shuffling + "Games"];
  document.body.style.cursor = "auto";
  div.html("");

  function cssColorFromProbability(p, games = 5000, maxOpacity = 1) {
    if (p === null) return `rgba(145, 121, 97, ${maxOpacity / 2})`;
    if (p >= 0.5)
      return `rgba(0, 255, 0, ${maxOpacity * Math.min(games / 5000, 1)})`;
    let q = 1 - p;
    // Exponential curve chosen to heavily emphasize the greater extremes of improbability.
    // These values are fitted to approximate the points (0.5, 0), (0.9, 0.5), (1, 1).
    // That is, at 50% improbability it's still fully green, at 90% it's halfway to red, and
    // the remaining 10% covers the other half of the distance.
    let progressToRed =
      0.001467411482151848 * Math.pow(708.081919401527, q) -
      0.03904753883392028;
    // The formula is designed to fit within the 0 to 1 range anyway, but in case of rounding
    // at the edges, explicitly cap it.
    progressToRed = Math.min(Math.max(progressToRed, 0), 1);
    // First ramp up red until a bright yellow at 0.5, then ramp down green.
    let r = Math.round(Math.min(progressToRed * 2, 1) * 255);
    let g = Math.round(Math.min((1 - progressToRed) * 2, 1) * 255);
    return `rgba(${r}, ${g}, 0, ${maxOpacity * Math.min(games / 5000, 1)})`;
  }

  if (viewTracker.viewing !== "table") {
    let v = viewTracker.viewing;
    if (!pathToVal(tableStats, v.column, v.row)) {
      viewTracker.viewing = "table";
    }
  }

  if (viewTracker.viewing === "table") {
    let columnKeys = [];
    tableStats.forEach((_, columnKey) => columnKeys.push(columnKey));
    let rowKeysSet = new Set();
    columnKeys.forEach(key =>
      tableStats[key].forEach((_, rowKey) => rowKeysSet.add(rowKey))
    );
    let rowKeys = [...rowKeysSet].sort(compareNumbers);

    let gameNumberDiv = $('<div class="stats_params"></div>');
    gameNumberDiv.append(
      '<label class="stats_param_label">' +
        `Total ${unitLabel.capitalize()} Included: ${totalGames}</label>`
    );
    addQueryCheckbox(
      gameNumberDiv,
      "Show numbers of " + unitLabel,
      viewTracker,
      "showGameNumbers",
      () => updateTable(params)
    );
    gameNumberDiv.css("grid-area", `1/1/span 1/span ${columnKeys.length + 1}`);
    gameNumberDiv.appendTo(div);

    let columnsHeaderDiv = $(`<div class="stats_title">${columnsHeader}</div>`);
    columnsHeaderDiv.css(
      "grid-area",
      `2/2/span 1/span ${columnKeys.length + 1}`
    );
    columnsHeaderDiv.appendTo(div);
    let rowsHeaderDiv = $(`<div class="stats_title">${rowsHeader}</div>`);
    rowsHeaderDiv.css("grid-area", `3/1/span ${rowKeys.length + 1}/span 1`);
    rowsHeaderDiv.appendTo(div);

    columnKeys.forEach((columnKey, columnIndex) => {
      let columnStats = tableStats[columnKey];
      let headerCell = $(`<div class="stats_column_header">${columnKey}</div>`);
      headerCell.css("grid-area", `3/${columnIndex + 3}`);
      headerCell.appendTo(div);
      rowKeys.forEach((rowKey, rowIndex) => {
        let cellStats = columnStats[rowKey];
        let cell = $('<div class="stats_cell"></div>');
        let numGames = (cellStats && cellStats.numGames) || 0;
        if (numGames > 0) {
          cell.addClass(
            viewTracker.showGameNumbers ? "stats_number_link" : "stats_link"
          );
          if (viewTracker.showGameNumbers) {
            cell.append(`<span>${numGames}</span>`);
          }
          let cellChance = cellStats.chance;
          let normalColor = cssColorFromProbability(cellChance, numGames, 0.6);
          let hoverColor = cssColorFromProbability(cellChance, numGames, 0.8);
          cell.css("background-color", normalColor);
          cell.hover(
            () => cell.css("background-color", hoverColor),
            () => cell.css("background-color", normalColor)
          );
          cell.click(() => {
            viewTracker.viewing = { column: columnKey, row: rowKey };
            updateTable(params);
          });
        }
        cell.css("grid-area", `${rowIndex + 4}/${columnIndex + 3}`);
        cell.appendTo(div);
      });
    });
    rowKeys.forEach((rowKey, rowIndex) => {
      let headerCell = $(`<div class="stats_row_header">${rowKey}</div>`);
      headerCell.css("grid-area", `${rowIndex + 4}/2`);
      headerCell.appendTo(div);
    });
  } else {
    let back = $('<div class="button back"></div>');
    back.css("grid-area", "1/1/span 2");
    back.click(() => {
      viewTracker.viewing = "table";
      updateTable(params);
    });
    back.appendTo(div);
    let chartSelectorDiv = $('<div class="chart_selector"></div>');
    chartSelectorDiv.css("grid-area", "3/1/span 16");
    let columnStats = tableStats[viewTracker.viewing.column];
    let cellStats = columnStats[viewTracker.viewing.row];
    let chartsStats =
      cellStats[viewTracker.extrapolated ? "extrapolated" : "known"];
    let chartIndex = viewTracker.viewing.chartIndex;
    for (let i = 0; i < chartsStats.length; i++) {
      let numGames = Math.round(
        chartsStats[i].counts.map(c => c.count).reduce(add)
      );
      if (numGames === 0 && i !== 0) break;
      let cell = $(
        `<div class="chart_select_cell">${chartSelectorLabelFunc(i)}</div>`
      );
      cell.click(() => {
        viewTracker.viewing.chartIndex = i;
        viewTracker.viewing.scrolledTo = chartSelectorDiv.scrollTop();
        updateTable(params);
      });
      if (i === chartIndex) {
        let color = cssColorFromProbability(chartsStats[i].chance, numGames, 1);
        cell.css("background-color", color);
      } else {
        let normalColor = cssColorFromProbability(
          chartsStats[i].chance,
          numGames,
          0.6
        );
        let hoverColor = cssColorFromProbability(
          chartsStats[i].chance,
          numGames,
          0.8
        );
        cell.css("background-color", normalColor);
        cell.hover(
          () => cell.css("background-color", hoverColor),
          () => cell.css("background-color", normalColor)
        );
      }
      cell.appendTo(chartSelectorDiv);
    }
    chartSelectorDiv.appendTo(div);
    if (viewTracker.viewing.scrolledTo) {
      chartSelectorDiv.scrollTop(viewTracker.viewing.scrolledTo);
    }

    if (chartIndex !== undefined) {
      let showNumbersDiv = $('<div class="stats_params"></div>');
      addQueryCheckbox(
        showNumbersDiv,
        "Show all numbers",
        viewTracker,
        "showBarNumbers",
        () => updateTable(params)
      );
      showNumbersDiv.css("grid-area", "1/2/span 1/span 8");
      showNumbersDiv.appendTo(div);

      let chartStats = chartsStats[chartIndex];
      let population = viewTracker.viewing.column;
      let hitsInPop = viewTracker.viewing.row;
      let sample = chartStats.counts.length - 1;
      if (sample === hitsInPop) sample = chartIndex + 1;
      let missesInPop = population - hitsInPop;
      let minCount = Math.max(sample - missesInPop, 0);
      let maxCount = Math.min(sample, hitsInPop);
      let counts = [];
      let numGames = 0;
      for (let i = minCount; i <= maxCount; i++) {
        counts.push(i);
        numGames += chartStats.counts[i].count;
      }
      numGames = Math.round(numGames);
      let actualStats = counts.map(count => chartStats.counts[count].count);
      let expectedDistribution = hypergeometricDistribution(
        population,
        sample,
        hitsInPop
      );
      let expectedStats = counts.map(
        count => numGames * expectedDistribution[count]
      );

      let dataSets = [[], [], []];
      let colorSets = [[], [], []];
      let markerWidth = Math.max(...actualStats, ...expectedStats) / 150;
      counts.forEach(count => {
        let actual = actualStats[count];
        let expected = expectedStats[count];

        if (actual < expected) {
          dataSets[0].push(actual);
          colorSets[0].push("#fae5d2");
          dataSets[1].push(Math.max(expected - actual - markerWidth / 2, 0));
          colorSets[1].push("rgba(0, 0, 0, 0)");
          dataSets[2].push(markerWidth);
          colorSets[2].push("red");
        } else {
          dataSets[0].push(Math.max(expected - markerWidth / 2, 0));
          colorSets[0].push("#fae5d2");
          dataSets[1].push(markerWidth);
          colorSets[1].push("red");
          dataSets[2].push(Math.max(actual - expected - markerWidth / 2, 0));
          colorSets[2].push("#fae5d2");
        }
      });

      function tooltip(index) {
        // The replace call removes trailing zeros.
        let numberWidth = String(
          Math.round(Math.max(actualStats[index], expectedStats[index]))
        ).length;
        function format(value) {
          let unpadded = math.format(value, {
            notation: "fixed",
            precision: 2
          });
          return ("      " + unpadded)
            .slice(-3 - numberWidth)
            .replace(/([0-9]+(\.[0-9]+[1-9])?)(\.?0+$)/, "$1");
        }
        let tooltip = [
          format(actualStats[index]) + " Actual",
          format(expectedStats[index]) + " Expected"
        ];
        // For the percentage chance, always show the full 3 decimals.
        if (chartStats.counts[counts[index]].chance !== null) {
          tooltip.push(
            math.format(chartStats.counts[counts[index]].chance * 100, {
              notation: "fixed",
              precision: 3
            }) + "% chance*"
          );
        }
        return tooltip;
      }

      let canvas = $("<canvas></canvas>");
      let graphDiv = $('<div class="graph shuffler_stats"></div>');
      graphDiv.css("grid-area", "2/2/span 17/span 8");
      canvas.appendTo(graphDiv);
      let chart = new Chart(canvas, {
        type: "bar",
        data: {
          xLabels: counts,
          datasets: [
            {
              data: dataSets[0],
              backgroundColor: colorSets[0],
              borderWidth: 0,
              hoverBackgroundColor: colorSets[0],
              hoverBorderWidth: 0,
              datalabels: {
                display: false
              }
            },
            {
              data: dataSets[1],
              backgroundColor: colorSets[1],
              borderWidth: 0,
              hoverBackgroundColor: colorSets[1],
              hoverBorderWidth: 0,
              datalabels: {
                display: false
              }
            },
            {
              data: dataSets[2],
              backgroundColor: colorSets[2],
              borderWidth: 0,
              hoverBackgroundColor: colorSets[2],
              hoverBorderWidth: 0,
              datalabels: {
                align: "top",
                anchor: "end",
                backgroundColor: "rgba(0, 0, 0, 0.8)",
                borderColor: "rgba(0, 0, 0, 0.8)",
                borderRadius: 6,
                borderWidth: 1,
                display: viewTracker.showBarNumbers,
                font: { family: "'Courier New', Courier, monospace" },
                formatter: (value, context) => tooltip(context.dataIndex)
              }
            }
          ]
        },
        options: {
          title: {
            display: true,
            fontColor: "#FAE5D2",
            fontFamily: "roboto",
            text: [
              chartTitleFunc(
                viewTracker.viewing.row,
                viewTracker.viewing.column
              ),
              `${numGames} ${unitLabel.capitalize()} Counted`
            ]
          },
          legend: {
            labels: {
              generateLabels: () => {
                return [
                  {
                    text:
                      `Actual ${unitLabel} recorded` +
                      (viewTracker.extrapolated ? " and extrapolated" : ""),
                    fillStyle: "#fae5d2",
                    strokeStyle: "#fae5d2"
                  },
                  {
                    text: "Expected amount from true random shuffle",
                    fillStyle: "red",
                    strokeStyle: "red"
                  }
                ];
              }
            }
          },
          tooltips: {
            mode: "x",
            intersect: false,
            bodyFontFamily: "'Courier New', Courier, monospace",
            callbacks: {
              beforeBody: items => tooltip(items[0].index),
              title: function() {},
              label: function() {}
            }
          },
          scales: {
            xAxes: [
              {
                type: "category",
                gridLines: {
                  display: false
                },
                ticks: {
                  fontColor: "#FAE5D2",
                  fontFamily: "roboto"
                },
                scaleLabel: {
                  display: true,
                  labelString: chartAxisLabelFunc(chartIndex),
                  fontColor: "#FAE5D2",
                  fontFamily: "roboto"
                },
                stacked: true
              }
            ],
            yAxes: [
              {
                type: "linear",
                ticks: {
                  fontColor: "#FAE5D2",
                  fontFamily: "roboto",
                  beginAtZero: true,
                  min: 0,
                  precision: 0
                },
                scaleLabel: {
                  display: true,
                  labelString: "Games",
                  fontColor: "#FAE5D2",
                  fontFamily: "roboto"
                },
                stacked: true
              }
            ]
          }
        }
      });

      graphDiv.append(
        '<div class="stats_footnote"><p>* Chance percentages are ' +
          "the probability of a true random shuffler producing results at least " +
          "this far from average.</p><p>" +
          "In math jargon, it is the binomial CDF for the range that does not " +
          "include the expected average, from an end to actual, inclusive, " +
          "doubled to make the scale 0% to 100%.</p></div>"
      );
      graphDiv.appendTo(div);
    } else {
      let promptDiv = $(
        '<div class="prompt">Select a chart from the left.</div>'
      );
      promptDiv.css("grid-area", "1/2/span 8/span 8");
      promptDiv.appendTo(div);
    }
  }
}

module.exports = {
  open_shuffler_tab: open_shuffler_tab,
  receiveStats: receiveStats
};
