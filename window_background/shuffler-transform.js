/*
globals
	add,
	cumulativeBinomial,
	hypergeometricDistribution
 */

function transformShufflerStats(stats) {
  let chartableStats = {};
  if (Array.isArray(stats)) {
    chartableStats.deckLandStats = transformDeckStats(stats);
  } else {
    chartableStats.landStats = transformLandStats(stats.lands);
    chartableStats.handStats = transformHandStats(stats.hands);
    chartableStats.cardStats = transformCardStats(stats.cards);
  }
  return chartableStats;
}

function transformLandStats(lands) {
  /*
	Each entry has this form:
	{
		date: <ISO8601 date string>,
		group: {
			librarySize: number,
			landsInLibrary: number,
			shuffling: <"standard" or "smoothed">
		},
		distribution: [ // outer index is how many cards from the top of the library
			[ // second index is how many lands were in the top (outer + 1) cards of the library
				[
					<number of games known to match the indices>,
					<number of games with 1 unknown card that would match the indices if it's not a land>
				]
			]
		]
	}

	Desired output form is:
	{
		date: <ISO8601 date string for most recent entry>
		smoothed: <same structure as standard>,
		smoothedGames: number of games that used smoothed shuffling,
		standard: [ // outer index is size of library (sparse array)
			[ // second index is number of lands in library (sparse array)
				{
					numGames: number,
					chance: <non-rigorous probability assessment for the overall set of extrapolated data>
					known: <same structure as extrapolated>
					extrapolated: [ // outer index is how many cards (0-based) from the top of the library
						{
							chance: <non-rigorous probability assessment for the overall inner distribution>,
							counts: [ // second index is how many lands were in those top cards
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
	 */
  let out = { date: "" };
  lands.forEach(entry => {
    if (entry.date > out.date) out.date = entry.date;
    let shuffling = entry.group.shuffling || "standard";
    if (!out[shuffling]) {
      out[shuffling] = [];
      out[shuffling + "Games"] = 0;
    }
    let shufflingStats = out[shuffling];
    let librarySize = entry.group.librarySize;
    let landsInLibrary = entry.group.landsInLibrary;
    let topNDistribution = transformTopNDistribution(
      entry.distribution,
      librarySize,
      landsInLibrary
    );
    if (!shufflingStats[librarySize]) {
      shufflingStats[librarySize] = [];
    }
    shufflingStats[librarySize][landsInLibrary] = topNDistribution;
    out[shuffling + "Games"] += topNDistribution.numGames;
  });
  return out;
}

function transformDeckStats(decks) {
  /*
	Each entry has this form:
	{
		date: <ISO8601 date string>,
		group: {
			deckSize: number,
			landsInDeck: number,
			bestOf: <1 or 3>
			librarySize: number,
			landsInLibrary: number,
			shuffling: <"standard" or "smoothed">
		},
		distribution: [ // outer index is how many cards from the top of the library
			[ // second index is how many lands were in the top (outer + 1) cards of the library
				[
					<number of games known to match the indices>,
					<number of games with 1 unknown card that would match the indices if it's not a land>
				]
			]
		]
	}

	Note that deckSize, landsInDeck, and bestOf will always be the same in every
	entry within a single call, and all other inputs are identical to the ones for
	land stats.

	Desired output form is:
	[ // outer index is size of deck (sparse array)
		[ // second index is number of lands in deck (sparse array)
			{
				<"1" or "3", stringified bestOf>: {
					smoothed: <same structure as standard>,
					smoothedGames: number of games that used smoothed shuffling,
					standard: [ // outer index is size of library (sparse array)
						[ // second index is number of lands in library (sparse array)
							{
								numGames: number,
								chance: <non-rigorous probability assessment for the overall set of extrapolated data>
								known: <same structure as extrapolated>
								extrapolated: [ // outer index is how many cards from the top of the library
									{
										chance: <non-rigorous probability assessment for the overall inner distribution>,
										counts: [ // second index is how many lands were in those top cards
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
			}
		]
	]

	Note that from bestOf on, this is identical to the output for land stats,
	except for omitting the date field.
	 */
  let shuffle = transformLandStats(decks);
  delete shuffle.date;
  let bestOf = {};
  bestOf[String(decks[0].group.bestOf)] = shuffle;
  let landsInDeck = [];
  landsInDeck[decks[0].group.landsInDeck] = bestOf;
  let deckSize = [];
  deckSize[decks[0].group.deckSize] = landsInDeck;
  return deckSize;
}

function transformHandStats(hands) {
  /*
	Each entry has this form:
	{
		date: <ISO8601 date string>,
		group: {
			deckSize: number,
			landsInDeck: number,
			bestOf: <1 or 3>
			shuffling: <"standard" or "smoothed">
		},
		distribution: [ // outer index is how many mulligans, always 0 to 6
			[ // second index is how many lands were in the drawn hand
				<number of games that match the indices>
			]
		]
	}

	Note that this is significantly different from the other inputs in that
	distribution is only a 2 dimensional array rather than 3. This is because
	there is no meaningful ability to extrapolate missing data, so what would be
	the second element of the innermost array is omitted, and the remaining single
	number is not wrapped in an array.

	Desired output form is:
	{
		date: <ISO8601 date string for most recent entry>
		<"1" or "3", stringified bestOf>: {
			smoothed: <same structure as standard>,
			smoothedGames: number of games that used smoothed shuffling,
			standard: [ // outer index is size of deck (sparse array)
				[ // second index is number of lands in deck (sparse array)
					{
						numGames: number,
						chance: <non-rigorous probability assessment for the overall set of extrapolated data>
						known: [ // outer index is number of mulligans, 0 to 6
							{
								chance: <non-rigorous probability assessment for the overall inner distribution>,
								counts: [ // second index is how many lands were in the drawn hand
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
	}

	Note that from bestOf on, this is identical to the output for land stats,
	except for omitting the extrapolation.
	 */
  let out = { date: "" };
  hands.forEach(entry => {
    if (entry.date > out.date) out.date = entry.date;
    let bestOf = String(entry.group.bestOf);
    if (!out[bestOf]) {
      out[bestOf] = {};
    }
    let bestOfStats = out[bestOf];
    let shuffling = entry.group.shuffling;
    if (!bestOfStats[shuffling]) {
      bestOfStats[shuffling] = [];
      bestOfStats[shuffling + "Games"] = 0;
    }
    let shufflingStats = bestOfStats[shuffling];
    let deckSize = entry.group.deckSize;
    if (!shufflingStats[deckSize]) {
      shufflingStats[deckSize] = [];
    }
    let deckSizeStats = shufflingStats[deckSize];
    let landsInDeck = entry.group.landsInDeck;
    let deckLandsStats = {
      numGames: entry.distribution[
        bestOf === "1" && shuffling === "standard" ? 1 : 0
      ].reduce(add),
      chance: null,
      known: []
    };
    bestOfStats[shuffling + "Games"] += deckLandsStats.numGames;
    deckSizeStats[landsInDeck] = deckLandsStats;
    for (let i = 0; i < 7; i++) {
      let expectedDistribution = hypergeometricDistribution(
        deckSize,
        7 - i,
        landsInDeck
      );
      deckLandsStats.known[i] = transformSingleDistribution(
        entry.distribution[i],
        expectedDistribution
      );
    }
    deckLandsStats.chance = combineProbabilitiesFromArray(deckLandsStats.known);
  });
  return out;
}

function transformCardStats(cards) {
  /*
	Each entry has this form:
	{
		date: <ISO8601 date string>,
		group: {
			type: <"first" or "all", for whether only the first card with this number
			  of copies in the decklist is counted>
			bestOf: <1 or 3>
			shuffling: <"standard" or "smoothed">,
			deckSize: number,
			copies: <2, 3, or 4>,
		},
		distribution: [ // outer index is how many cards from the top of the deck
			[ // second index is how many copies of the card were in the top (outer + 1) cards of the deck
				[
					<number of games known to match the indices>,
					<number of games with 1 unknown card that would match the indices if it's not the counted card>
				]
			]
		]
	}

	Note that, while the group information is different, the distribution field
	is identical to the one for land stats.

	Desired output form is:
	{
		date: <ISO8601 date string for most recent entry>,
		<"first" or "all">: {
			<"1" or "3", stringified bestOf>: {
				smoothed: <same structure as standard>,
				smoothedGames: number of games that used smoothed shuffling,
				standard: [ // outer index is size of deck (sparse array)
					[ // second index is number of copies of a card in deck (2, 3, or 4)
						{
							numGames: number,
							chance: <non-rigorous probability assessment for the overall set of extrapolated data>
							known: <same structure as extrapolated>
							extrapolated: [ // outer index is how many cards from the top of the deck
								{
									chance: <non-rigorous probability assessment for the overall inner distribution>,
									counts: [ // second index is how many copies of the counted card were in those top cards
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
		}
	}

	Note that from bestOf on, this is identical to the output for land stats,
	except for exactly what the indices of the arrays refer to.
	 */
  let out = { date: "" };
  cards.forEach(entry => {
    if (entry.date > out.date) out.date = entry.date;
    let cardSets = entry.group.type;
    if (!out[cardSets]) {
      out[cardSets] = {};
    }
    let cardSetsStats = out[cardSets];
    let bestOf = String(entry.group.bestOf);
    if (!cardSetsStats[bestOf]) {
      cardSetsStats[bestOf] = {};
    }
    let bestOfStats = cardSetsStats[bestOf];
    let shuffling = entry.group.shuffling || "standard";
    if (!bestOfStats[shuffling]) {
      bestOfStats[shuffling] = [];
      bestOfStats[shuffling + "Games"] = 0;
    }
    let shufflingStats = bestOfStats[shuffling];
    let deckSize = entry.group.deckSize;
    let copies = entry.group.copies;
    let topNDistribution = transformTopNDistribution(
      entry.distribution,
      deckSize,
      copies
    );
    if (!shufflingStats[deckSize]) {
      shufflingStats[deckSize] = [];
    }
    shufflingStats[deckSize][copies] = topNDistribution;
    bestOfStats[shuffling + "Games"] += topNDistribution.numGames;
  });
  return out;
}

function transformTopNDistribution(distribution, population, hitsInPop) {
  let extrapolated = [];
  let extrapolatedTotal = [];
  let known = [];
  distribution.forEach((sample, prevSampleSize) => {
    let expectedDistribution = hypergeometricDistribution(
      population,
      prevSampleSize + 1,
      hitsInPop
    );
    // extrapolated includes only the games that are being extrapolated
    extrapolated[prevSampleSize] = [];
    // extrapolatedCounts includes both extrapolated and definite known games
    let extrapolatedCounts = [];
    let knownCounts = [];
    sample.forEach((gameCounts, hitsInSample) => {
      if (hitsInSample > prevSampleSize + 1) return;
      // These numbers are for what was left as of the previous sample.
      let cardsLeft = population - prevSampleSize;
      let hitsLeft = hitsInPop - hitsInSample;
      let missesLeft = cardsLeft - hitsLeft;

      // The number of games that were already extrapolating in the previous
      // sample that, if the next card is a miss, would match hitsInSample, plus
      // the number that are just now entering extrapolation.
      let toExtrapolateMiss =
        prevSampleSize === 0
          ? gameCounts[1]
          : (extrapolated[prevSampleSize - 1][hitsInSample] || 0) +
            gameCounts[1];
      // Same as above, but for if the new unknown card is a hit instead.
      let toExtrapolateHit =
        hitsInSample === 0
          ? 0
          : prevSampleSize === 0
          ? sample[hitsInSample - 1][1]
          : extrapolated[prevSampleSize - 1][hitsInSample - 1] +
            sample[hitsInSample - 1][1];
      // Now account for the probability of each of those games actually getting
      // the hit or miss that would place it in this position in the data.
      let extrapolatedMisses = (toExtrapolateMiss * missesLeft) / cardsLeft;
      let extrapolatedHits = (toExtrapolateHit * (hitsLeft + 1)) / cardsLeft;
      extrapolated[prevSampleSize][hitsInSample] =
        extrapolatedHits + extrapolatedMisses;

      extrapolatedCounts[hitsInSample] =
        gameCounts[0] + extrapolatedHits + extrapolatedMisses;
      knownCounts[hitsInSample] = gameCounts[0];
    });
    extrapolatedTotal[prevSampleSize] = transformSingleDistribution(
      extrapolatedCounts,
      expectedDistribution
    );
    known[prevSampleSize] = transformSingleDistribution(
      knownCounts,
      expectedDistribution
    );
  });
  return {
    numGames: distribution[0][0][0] + distribution[0][1][0],
    // Base the assessment on extrapolation to prevent skew from games ending
    // early, but limit to the first 10 cards where extrapolation doesn't have
    // room to overwhelm actual data.
    chance: combineProbabilitiesFromArray(extrapolatedTotal.slice(0, 10)),
    known: known,
    extrapolated: extrapolatedTotal
  };
}

function transformSingleDistribution(counts, expectedDistribution) {
  let numGames = Math.round(counts.reduce(add));
  if (numGames === 0) {
    return {
      chance: null,
      counts: counts.map(e => ({ chance: null, count: 0 }))
    };
  }
  let countsWithOdds = counts.map((count, i) => {
    let expectedCount = numGames * (expectedDistribution[i] || 0);
    if (expectedCount === 0 || expectedCount === numGames) {
      return { chance: null, count: count };
    }
    // In case of extrapolation, don't let an above-average point early on get
    // extrapolated into an above-average fringe case resulting in a spurious
    // near-0 probability number. Leave the bounds unrounded, however, to take
    // advantage of the implementation of cumulativeBinomial which is
    // generalized to accept non-integer ranges. Double the returned result to
    // expand the above-average and below-average ranges back up to 0 to 1,
    // producing a proximity-to-average assessment.
    let useBottom = count < expectedCount || count < 0.5;
    let lowerBound = useBottom ? 0 : count;
    let upperBound = useBottom ? count : numGames;
    return {
      chance: Math.min(
        cumulativeBinomial(
          numGames,
          lowerBound,
          upperBound,
          expectedDistribution[i]
        ) * 2,
        1
      ),
      count: count
    };
  });
  return {
    chance: combineProbabilitiesFromArray(countsWithOdds),
    counts: countsWithOdds
  };
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

module.exports = {
  transformShufflerStats: transformShufflerStats
};
