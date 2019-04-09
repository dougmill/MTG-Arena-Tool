/*
 * Data for individual games is in the "gameStats" array field of each match.
 * A game is included in the aggregation if all of the following are true:
 *
 * - The game has a "handsDrawn" field, and it is not empty and has no empty
 *   elements. If it does not, then the relevant data is not present to be
 *   analyzed. This can happen either for matches played before the code to
 *   record that data was added, or for matches conceded during mulligan.
 * - The game is in a best of 3 match. If it is not, the opening hand was
 *   affected by the Bo1 algorithm which might bias the data.
 * - The match either does not have a "toolRunFromSource" field or that field's
 *   value is not true. This is to prevent untested in-development changes from
 *   accidentally adding inaccurate data.
 * - The game either is the first one in the match or has a "deck" field.
 *   Otherwise its decklist may have been changed by sideboarding and the
 *   information required to determine it was not recorded.
 * - The game's deck size is either 40 or 60.
 * - The match either has a "toolVersion" field with value at least 131605 or a
 *   date earlier than March 27 noon UTC. Otherwise it was read with a version
 *   of Tool that did not properly understand the decklist format in use at the
 *   time.
 * - It is possible to unambiguously detect whether a particular card is one of
 *   the ones considered relevant.
 *
 * The form of the output documents is:
 * {
 *   "group": {
 *     // which end of the decklist is considered relevant for this distribution
 *     "end": "front",
 *     // how many cards are considered relevant
 *     "numCards": 22,
 *     // which version of Arena these games were played in, determined by
  *    // comparing date with the update release times
 *     "update": 13
 *   },
 *   // the most recent date of any game included in this distribution
 *   "date": "2019-04-09T05:05:07.480Z",
 *   // distribution[mulligans][relevantCardsInHand] = number of games
 *   "distribution": [
 *     [0, 1, 0, 0, 2, 0, 0, 0],
 *     [0, 1, 0, 1, 0, 0, 0],
 *     [0, 0, 0, 0, 0, 0],
 *     [0, 0, 0, 0, 0],
 *     [0, 0, 0, 0],
 *     [0, 0, 0],
 *     [0, 0]
 *   ]
 * }
 */
db.getCollection("matches").aggregate(

  // Pipeline
  [
    // Stage 1
    {
      $match: { $and: [{ date: { $gt: "2019-01-01T00:00:00.000Z" } }, { date: { $lt: "2019-04-29T00:00:00.000Z" } }] }
    },

    // Stage 2
    {
      $match: {
        // The Bo1 opening hand algorithm, even though it is aimed at lands, could still throw off the distributions
        // of position-in-decklist based stats. To prevent that, include only Bo3 games.
        "gameStats.0.handsDrawn": { $exists: true },
        bestOf: NumberInt(3),
        // When Tool is run from source, it may contain experimental or in-development changes that have not been
        // fully tested and reviewed to ensure they do not produce inaccurate or incorrectly formatted data.
        toolRunFromSource: { $ne: true }
      }
    },

    // Stage 3
    {
      $sort: { date: NumberInt(1) }
    },

    // Stage 4
    {
      $limit: NumberInt(2000)
    },

    // Stage 5
    {
      // By this point, we have the (at most) 2000 earliest matches that both are in the target date range and have
      // shuffler data. Now trim out the non relevant fields.
      $project: {
        _id: NumberInt(0),
        date: NumberInt(1),
        gameStats: NumberInt(1),
        toolVersion: NumberInt(1),
        deckList: "$playerDeck.mainDeck"
      }
    },

    // Stage 6
    {
      // gameStats is where all the shuffler data is.
      $unwind: {
        path : "$gameStats",
        includeArrayIndex: "gameIndex"
      }
    },

    // Stage 7
    {
      $project: {
        date: NumberInt(1),
        // It is possible for the decklist to change, even drastically, in sideboarding. The sideboarded decklist is
        // now included in the stats for games 2 and 3, replace the base decklist with it for those games.
        deckList: {
          $cond: [{ $eq: ["$gameIndex", NumberInt(0)] }, "$deckList", "$gameStats.deck.mainDeck"]
        },
        deckSize: "$gameStats.deckSize",
        handsDrawn: "$gameStats.handsDrawn",
        toolVersion: NumberInt(1),
      }
    },

    // Stage 8
    {
      $match: {
        // The statistics for this aggregation depend heavily on the precise details of the decklist, which cannot
        // be reliably derived for sideboarded games from data recorded before the sideboarded decklist was added to
        // match records.
        deckList: { $exists: true },
        // The overwhelming majority of games have either 40 or 60 cards, to the point where there would be little
        // benefit from analyzing this aggregation for other deck sizes.
        deckSize: { $in: [NumberInt(40), NumberInt(60)] },
        // The March update to Arena changed the format of most logged decklists, requiring a hotfix update to Tool
        // to handle it. Any games recorded by the previous version of Tool after Arena updated almost certainly
        // have missing or incorrect decklists, so filter them out.
        $or: [{
          "toolVersion": { $gte: NumberInt(131605) }
        },{
          "date": { $lt: "2019-03-27T12:00:00.000Z" }
        }],
        // Sometimes, such as on concede during mulligan, the opening hand is not known and gets recorded as being
        // empty. Filter such games out to prevent them being counted as having 0 relevant cards in hand.
        handsDrawn: { $ne: [] }
      }
    },

    // Stage 9
    {
      $project: {
        date: NumberInt(1),
        // In case WotC changed shuffling without mentioning it in release notes, keep separate records for each
        // Arena release. Tool began recording this data in January, when Arena version 0.11 was current.
        update: {
          $switch: {
            branches: [{
              case: { $lt: ["$date", "2019-02-14T14:00:00.000Z"] },
              then: NumberInt(11),
            },{
              case: { $lt: ["$date", "2019-03-27T12:00:00.000Z"] },
              then: NumberInt(12),
            }],
            default: NumberInt(13)
          }
        },
        // The decklist format was not designed with this kind of analysis in mind, so it's a bit inconvenient to
        // work with. Additionally, the fact that copies of a card cannot be distinguished from each other means
        // that every card included in the range to be analyzed must have ALL of its copies in the range.
        // Fortunately, copies of a card are nearly always grouped together by Arena - "nearly" because importing
        // a decklist with the same card listed in multiple places will have them separated in the resulting deck,
        // but even just opening the deck and saving it again - without altering it - will combine them into one.
        // The format of the decklist is:
        //      [{ id: "68310", quantity: 4 },
        //       { id: "68096", quantity: 3},
        //       ...]
        //
        // stats will be an array of objects, structured as:
        //      [
        //          // Index has no real meaning, it's just a collection of objects.
        //          {
        //              end: <'front' or 'back'>,
        //              numCards: <total number of cards, accounting for quantity, in the range being considered>,
        //              handCounts: <array of how many cards from that range were in each drawn hand>
        //          }
        //      ]
        stats: {
          $let: {
            vars: {
              bounds: {
                // First, find a suitable range of cards, if possible, at each of the front and the back.
                // For 40 card decks, look for 15 to 18 cards. For 60 card decks, look for 22 to 25 cards.
                // If there are multiple acceptable ranges to choose from, prefer the option closest to
                // 17 or 24, breaking ties by choosing the smaller one. This reduce operation scans through
                // the decklist, counting cards and moving the bounds as it goes, until it finds the best
                // option and stops moving each bound.
                $reduce: {
                  input: "$deckList",
                  initialValue: {
                    // The index in the decklist of the card group being examined.
                    index: NumberInt(0),
                    // The number of cards, accounting for quantity, that have already been checked.
                    cards: NumberInt(0),
                    // The smallest acceptable number of cards.
                    minTarget: { $cond: [{ $eq: ["$deckSize", NumberInt(40)] }, NumberInt(15), NumberInt(22)] },
                    // The largest acceptable number of cards.
                    maxTarget: { $cond: [{ $eq: ["$deckSize", NumberInt(40)] }, NumberInt(18), NumberInt(25)] },
                    // Index where the front range ends, exclusive.
                    frontBound: NumberInt(0),
                    // Index where the back range starts, inclusive.
                    backBound: NumberInt(0)
                  },
                  in: {
                    // Track the index as we go.
                    index: { $add: ["$$value.index", NumberInt(1)] },
                    cards: { $add: ["$$value.cards", "$$this.quantity"] },
                    minTarget: "$$value.minTarget",
                    maxTarget: "$$value.maxTarget",
                    frontBound: {
                      $switch: {
                        branches: [{
                          // First branch: Is it time to stop moving the front bound?
                          case: {
                            $or: [{
                              // If not equal, then it's already been stopped.
                              $ne: ["$$value.index", "$$value.frontBound"]
                            },{
                              // If moving it would go past the acceptable range, and
                              // it is currently in the acceptable range, then stop.
                              $and: [{
                                $gt: [{ $add: ["$$value.cards", "$$this.quantity"] }, "$$value.maxTarget"]
                              },{
                                $gte: ["$$value.cards", "$$value.minTarget"]
                              }]
                            },{
                              // If already in acceptable range, don't go to the max
                              // of the range from anywhere except the min - anywhere
                              // else is either the ideal spot or wins the tiebreaker.
                              $and: [{
                                $gt: ["$$value.cards", "$$value.minTarget"]
                              },{
                                $eq: [{ $add: ["$$value.cards", "$$this.quantity"] }, "$$value.maxTarget"]
                              }]
                            }]
                          },
                          // Front bound either has already stopped or should be stopped now.
                          then: "$$value.frontBound"
                        },{
                          // Second branch: We're not in the acceptable range yet, otherwise
                          // the first branch would have been taken. Does this card skip over
                          // the acceptable range entirely?
                          case: { $gt: [{ $add: ["$$value.cards", "$$this.quantity"] }, "$$value.maxTarget"] },
                          // Use an invalid value to mark that there's no acceptable range.
                          then: NumberInt(-1)
                        }],
                        // Not time to stop, haven't gone too far, so keep it moving.
                        default: { $add: ["$$value.frontBound", NumberInt(1)] }
                      }
                    },
                    backBound: {
                      $switch: {
                        branches: [{
                          // First branch: Is it time to stop moving the back bound?
                          case: {
                            $or: [{
                              // If not equal, then it's already been stopped.
                              $ne: ["$$value.index", "$$value.backBound"]
                            },{
                              // If moving it would go past the acceptable range, and
                              // it is currently in the acceptable range, then stop.
                              $and: [{
                                $gt: [{ $add: ["$$value.cards", "$$this.quantity"] }, { $subtract: ["$deckSize", "$$value.minTarget"] }]
                              },{
                                $gte: ["$$value.cards", { $subtract: ["$deckSize", "$$value.maxTarget"] }]
                              }]
                            },{
                              // If at exactly the max acceptable range, and this card
                              // would move to the min - the only value less preferred
                              // - then stop.
                              $and: [{
                                $eq: ["$$value.cards", { $subtract: ["$deckSize", "$$value.maxTarget"] }]
                              },{
                                $eq: ["$$this.quantity", NumberInt(3)]
                              }]
                            },{
                              // If in the acceptable range and not at the max of it,
                              // then stop.
                              $gt: ["$$value.cards", { $subtract: ["$deckSize", "$$value.maxTarget"] }]
                            }]
                          },
                          // Back bound either has already stopped or should be stopped now.
                          then: "$$value.backBound"
                        },{
                          // Second branch: We're not in the acceptable range yet, otherwise
                          // the first branch would have been taken. Does this card skip over
                          // the acceptable range entirely?
                          case: { $gt: [{ $add: ["$$value.cards", "$$this.quantity"] }, { $subtract: ["$deckSize", "$$value.minTarget"] }] },
                          // Use an invalid value to mark that there's no acceptable range.
                          then: NumberInt(-1)
                        }],
                        // Not time to stop, haven't gone too far, so keep it moving.
                        default: { $add: ["$$value.backBound", NumberInt(1)] }
                      }
                    }
                  }
                }
              },
              quantities: "$deckList.quantity",
              // Older decklists have card ids as strings, but in opening hands and most other places they're
              // integers. Do the conversion so comparisons will work.
              ids: {
                $map: {
                  input: "$deckList.id",
                  as: "cardId",
                  in: { $toInt: "$$cardId" }
                }
              }
            },
            in: {
              $map: {
                input: {
                  // Generate an entry for the front and for the back, but filter out invalid ones.
                  $filter: {
                    input: [{
                      end: "front",
                      bound: "$$bounds.frontBound",
                      numCards: { $sum: { $slice: ["$$quantities", "$$bounds.frontBound"] } },
                      idsToCheck: { $slice: ["$$ids", "$$bounds.frontBound"] },
                      // There should be vanishingly few games with the same card in multiple
                      // places in the decklist, but check anyway just in case.
                      ambiguousIds: {
                        $setIntersection: [{
                          $slice: ["$$ids", "$$bounds.frontBound"]
                        },{
                          $slice: ["$$ids", "$$bounds.frontBound", NumberInt(60)]
                        }]
                      }
                    }, {
                      end: "back",
                      bound: "$$bounds.backBound",
                      numCards: { $sum: { $slice: ["$$quantities", "$$bounds.backBound", NumberInt(60)] } },
                      idsToCheck: { $slice: ["$$ids", "$$bounds.backBound", NumberInt(60)] },
                      // There should be vanishingly few games with the same card in multiple
                      // places in the decklist, but check anyway just in case.
                      ambiguousIds: {
                        $setIntersection: [{
                          $slice: ["$$ids", "$$bounds.backBound"]
                        },{
                          $slice: ["$$ids", "$$bounds.backBound", NumberInt(60)]
                        }]
                      }
                    }],
                    as: "info",
                    cond: {
                      // bound = -1 is used to indicate there's no acceptable number of cards that
                      // will work. If ambiguousIds is not empty, then there's a card that has copies
                      // both inside and outside the relevant section of the decklist. In either case,
                      // drop the entry.
                      $and: [{
                        $ne: ["$$info.bound", NumberInt(-1)]
                      },{
                        $eq: [{ $size: "$$info.ambiguousIds" }, NumberInt(0)]
                      }]
                    }
                  }
                },
                as: "info",
                in: {
                  end: "$$info.end",
                  numCards: "$$info.numCards",
                  // handsDrawn in the input is a 2 dimensional array, with each 1 dimensional array being
                  // a hand that was kept or mulliganed, and each value in a hand being a card id. Reduce
                  // each hand to a count of how many cards in it came from the region of the decklist
                  // being considered.
                  handCounts: {
                    $map: {
                      input: "$handsDrawn",
                      as: "hand",
                      in: {
                        $size: {
                          $filter: {
                            input: "$$hand",
                            as: "card",
                            cond: { $in: ["$$card", "$$info.idsToCheck"] }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },

    // Stage 10
    {
      $unwind: {
        path : "$stats",
      }
    },

    // Stage 11
    {
      $group: {
        _id: {
          end: "$stats.end",
          numCards: "$stats.numCards",
          update: "$update"
        },
        date: { $max: "$date" },
        // stats.handCounts from the previous projection is a 1 dimensional array, with each value being the number
        // of cards from one end of the decklist that were in a hand drawn in the game, in the same order as the
        // hands were drawn.
        handCounts: { $push: "$stats.handCounts" }
      }
    },

    // Stage 12
    {
      $project: {
        date: NumberInt(1),
        // distribution will be a 2 dimensional array, structured as:
        //      [
        //          // First index is number of mulligans.
        //          [
        //              // Second index is number of cards from the front/back of the decklist in the drawn hand.
        //              <Number of games that match the indices>
        //          ]
        //      ]
        distribution: {
          $map: {
            // Size of the hand at each value of the outer index
            input: { $range: [NumberInt(7), NumberInt(0), NumberInt(-1)] },
            as: "handSize",
            in: {
              $map: {
                // Inner index, number of cards from the relevant end of the decklist in the hand. Cover
                // every value from 0 lands to the entire hand.
                input: { $range: [NumberInt(0), { $add: ["$$handSize", NumberInt(1)] }] },
                as: "count",
                in: {
                  // Count number of games that match the indices.
                  $size: {
                    $filter: {
                      input: "$handCounts",
                      as: "hand",
                      cond: {
                        $eq: [{ $arrayElemAt: ["$$hand", { $subtract: [NumberInt(7), "$$handSize"] }] }, "$$count"]
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    },

    // Stage 13
    {
      // Need to merge with previously aggregated data.
      $lookup: {
        from: "position_stats",
        localField: "_id",
        foreignField: "_id",
        as: "stats"
      }
    },

    // Stage 14
    {
      $project: {
        date: NumberInt(1),
        distribution: {
          // If there is no previously aggregated data for this group, then save some time and skip the merge.
          $cond: [{ $eq: [{ $size: "$stats" }, NumberInt(0)] }, "$distribution", {
            $map: {
              // Pair up same-mulligans 1 dimensional inner arrays.
              // There can only ever be one matching doc to merge with, so extract it.
              input: { $zip: { inputs: ["$distribution", { $arrayElemAt: ["$stats.distribution", NumberInt(0)] }] } },
              as: "handSizeStats",
              in: {
                $map: {
                  // Pair up the actual values that have both indices matching, one value from new
                  // data, one from old.
                  input: { $zip: { inputs: [{ $arrayElemAt: ["$$handSizeStats", NumberInt(0)] }, { $arrayElemAt: ["$$handSizeStats", NumberInt(1)] }] } },
                  as: "countStats",
                  // Finally, add the paired-up values.
                  in: { $sum: "$$countStats" }
                }
              }
            }
          }]
        }
      }
    },

    // Stage 15
    {
      $out: "position_stats"
    },

  ]

  // Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
