/*
 * Data for individual games is in the "gameStats" array field of each match.
 * A game is included in the aggregation if all of the following are true:
 *
 * - The game has a "handsDrawn" field, and it is not empty and has no empty
 *   elements. If it does not, then the relevant data is not present to be
 *   analyzed. This can happen either for matches played before the code to
 *   record that data was added, or for matches conceded before that data was
 *   recorded.
 * - The game is in a best of 3 match. If it is not, the opening hand was
 *   affected by the Bo1 algorithm which might bias the data.
 * - The match either does not have a "toolRunFromSource" field or that field's
 *   value is not true. This is to prevent untested in-development changes from
 *   accidentally adding inaccurate data.
 * - The game either is the first one in the match or has a "deck" field.
 *   Otherwise its decklist may have been changed by sideboarding and the
 *   information required to determine it was not recorded.
 * - The game's deck size is 60.
 * - The match either has a "toolVersion" field with value at least 131605 or a
 *   date earlier than March 27 noon UTC. Otherwise it was read with a version
 *   of Tool that did not properly understand the decklist format in use at the
 *   time.
 * - It does not have multiple separate groups of the same card (including same
 *   set and same art) in the decklist.
 * - It is possible to unambiguously detect whether a particular card is one of
 *   the ones considered relevant.
 *
 * The form of the output documents is:
 * {
 *   "group": {
 *     // How many cards are considered relevant, or omitted for estimations
 *     // that assume a card is equally likely to be any of its copies.
 *     "numCards": 4,
 *     // Identifier for the time span of included games. All games before
 *     // 2019-02-07T00:00:00.000Z are considered week 0 (data collection
 *     // started Jan 28), with each interval after that covering one week.
 *     "week": 2,
 *   },
 *   // The most recent date of any game included in this distribution.
 *   "date": "2019-04-09T05:05:07.480Z",
 *   // distribution[mulligans][positionOfFirstRelevant][relevantCardsInHand] =
 *   //     number of games
 *   // If numCards is less than 7 or omitted, the values at the end of the
 *   // inner arrays that cannot possibly be non-zero are omitted, shortening
 *   // the arrays.
 *   "distribution": [
 *     [
 *       [0, 1, 0, 0, 2, 0, 0, 0],
 *       ...
 *     ],[
 *       [0, 1, 0, 1, 0, 0, 0],
 *       ...
 *     ],[
 *       [0, 0, 0, 0, 0, 0],
 *       ...
 *     ],[
 *       [0, 0, 0, 0, 0],
 *       ...
 *     ],[
 *       [0, 0, 0, 0],
 *       ...
 *     ],[
 *       [0, 0, 0],
 *       ...
 *     ],[
 *       [0, 0],
 *       ...
 *     ]
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
        "gameStats.0.handsDrawn": { $exists: true },
        // The Bo1 opening hand algorithm, even though it is aimed at lands, could still throw off the distributions
        // of position-in-decklist based stats. To prevent that, include only Bo3 games.
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
      $limit: NumberInt(1000)
    },

    // Stage 5
    {
      // By this point, we have the (at most) 1000 earliest matches that both are in the target date range and have
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
        // The overwhelming majority of games have 60 cards, to the point where there would be little benefit from
        // analyzing this aggregation for other deck sizes.
        deckSize: NumberInt(60),
        // The March update to Arena changed the format of most logged decklists, requiring a hotfix update to Tool
        // to handle it. Any games recorded by the previous version of Tool after Arena updated almost certainly
        // have missing or incorrect decklists, so filter them out.
        $or: [{
          "toolVersion": { $gte: NumberInt(131605) }
        },{
          "date": { $lt: "2019-03-27T12:00:00.000Z" }
        }],
        // Sometimes, if the game is conceded very early, the opening hand is not known and gets recorded as being
        // empty. Filter such games out to prevent them being counted as having 0 relevant cards in hand.
        handsDrawn: { $ne: [] },
        // There should be vanishingly few games with the same card in multiple places in the decklist, but check anyway
        // just in case. Keep only games with the same number of unique cards as entries in the decklist.
        $expr: {
          $eq: [
            { $size: "$deckList" },
            {
              $size: {
                // Find unique card ids. Have to work around the fact that the various set operators only work with an
                // array of expressions, not an expression that evaluates to an array. So, using $reduce.
                $reduce: {
                  input: "$deckList",
                  initialValue: [],
                  in: {
                    $setUnion: ["$$value", ["$$this.id"]]
                  }
                }
              }
            }
          ]
        }
      }
    },

    // Stage 9
    {
      $project: {
        date: NumberInt(1),
        // To be sure of being able to notice an unannounced fix promptly, keep separate records for each week. Split
        // the weeks at midnight UTC Thursday morning, joining the fragment of a week following the release that started
        // recording opening hands with the following week.
        week: {
          $max: [
            {
              $toInt: {
                $ceil: {
                  $divide: [
                    // Feb 7 is the Thursday following the first full week of opening hands being recorded.
                    {
                      $subtract: [
                        { $dateFromString: { dateString: "$date" } },
                        { $dateFromString: { dateString: "2019-02-07T00:00:00.000Z" } }
                      ]
                    },
                    // Number of milliseconds in a week. 7 * 24 * 60 * 60 * 1000
                    NumberInt(604800000)
                  ]
                }
              }
            },
            // Anything before Feb 7 is week 0.
            NumberInt(0)
          ]
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
        //              // numCards omitted for estimations that assume a card is equally likely to be any of its copies
        //              numCards: <total number of cards, accounting for quantity, that are considered relevant at each
        //                         position>,
        //              distribution: [ // index is number of mulligans
        //                  [ // index is 0-based position in the decklist of the first relevant card
        //                      [ // index is number of relevant cards in the drawn hand
        //                          <0 or 1 for whether this game matches, or a fraction if doing estimations and the
        //                           card has multiple copies>
        //                      ]
        //                  ]
        //              ]
        //          }
        //      ]
        stats: {
          $let: {
            vars: {
              // Using the example decklist, this will be [68310, 68310, 68310, 68310, 68096, 68096, 68096, ...]. Having
              // this form greatly simplifies position-based checks.
              deckArray: {
                $reduce: {
                  input: "$deckList",
                  initialValue: [],
                  in: {
                    $concatArrays: ["$$value", {
                      $map: {
                        input: { $range: [NumberInt(0), "$$this.quantity"] },
                        as: "ignored",
                        // Older decklists have card ids as strings, but in opening hands and most other places they're
                        // integers. Do the conversion so comparisons will work.
                        in: { $toInt: "$$this.id" }
                      }
                    }]
                  }
                }
              },
              // This is used in estimations to get stats for every position from *every* game, on the assumption of a
              // correct shuffler where any drawn copy of a card would be equally likely to be any of the copies that
              // went into the shuffler. In such estimations, each card drawn has its 1 game split evenly among the
              // positions it could have come from - card weight per position is 1/quantity in decklist.
              cardEstimationWeights: {
                $reduce: {
                  input: "$deckList",
                  initialValue: [],
                  in: {
                    $concatArrays: ["$$value", {
                      $map: {
                        input: { $range: [NumberInt(0), "$$this.quantity"] },
                        as: "ignored",
                        in: { $divide: [NumberInt(1), "$$this.quantity"] }
                      }
                    }]
                  }
                }
              }
            },
            in: {
              $map: {
                // The various quantities of cards that I'm interested in. 0 is used to indicate doing the estimation
                // and will be filtered out later. Index is meaningless for this one.
                input: [NumberInt(0), NumberInt(1), NumberInt(2), NumberInt(3), NumberInt(4), NumberInt(22),
                        NumberInt(23), NumberInt(24), NumberInt(25)],
                as: "numCards",
                in: {
                  numCards: "$$numCards",
                  distribution: {
                    $map: {
                      // The outer index, number of mulligans from 0 to 6 inclusive.
                      input: { $range: [NumberInt(0), NumberInt(7)] },
                      as: "mulligans",
                      in: {
                        $map: {
                          // Middle index, position in the decklist of the first relevant card. If only 1 card is
                          // relevant, can range from 0 all the way to 59 inclusive. Each additional relevant card
                          // reduces the maximum position by 1.
                          input: {
                            $range: [NumberInt(0), { $subtract: [NumberInt(61), { $max: ["$$numCards", NumberInt(1)] }] }]
                          },
                          as: "position",
                          in: {
                            // Should this game count for this combination of numCards, mulligans, and position? And if
                            // doing estimation, what fractions should be counted for this position?
                            $cond: {
                              // numCards = 0 means doing estimation
                              if: { $eq: ["$$numCards", NumberInt(0)] },
                              then: {
                                $cond: {
                                  // If the game didn't have this many mulligans, don't count it for anything.
                                  if: { $lte: [{ $size: "$handsDrawn" }, "$$mulligans"] },
                                  then: [NumberInt(0), NumberInt(0)],
                                  else: {
                                    // How many copies of the card at this position were drawn?
                                    $let: {
                                      vars: {
                                        numDrawn: {
                                          // Get the hand for this mulligan, and count how many cards drawn match what's
                                          // at this position.
                                          $size: {
                                            $filter: {
                                              input: { $arrayElemAt: ["$handsDrawn", "$$mulligans"] },
                                              as: "card",
                                              cond: {
                                                $eq: ["$$card", { $arrayElemAt: ["$$deckArray", "$$position"] }]
                                              }
                                            }
                                          }
                                        }
                                      },
                                      // Take the number of copies drawn, and use the previously calculated weights to
                                      // spread them out among all copies that they could be.
                                      in: [
                                        {
                                          $subtract: [
                                            NumberInt(1),
                                            {
                                              $multiply: [
                                                "$$numDrawn",
                                                { $arrayElemAt: ["$$cardEstimationWeights", "$$position"] }
                                              ]
                                            }
                                          ]
                                        },{
                                          $multiply: [
                                            "$$numDrawn",
                                            { $arrayElemAt: ["$$cardEstimationWeights", "$$position"] }
                                          ]
                                        }
                                      ]
                                    }
                                  }
                                }
                              },
                              // Not doing estimation, need to generate an array with a 1 in the spot for the number of
                              // copies actually drawn.
                              else: {
                                $map: {
                                  // Inner index, number of relevant cards drawn. Can be up to either the number of
                                  // relevant cards in the deck or the size of the hand, whichever is smaller.
                                  input: {
                                    $range: [
                                      NumberInt(0),
                                      {
                                        $min: [
                                          { $add: ["$$numCards", NumberInt(1)] },
                                          { $subtract: [NumberInt(8), "$$mulligans"] }
                                        ]
                                      }
                                    ]
                                  },
                                  as: "numDrawn",
                                  in: {
                                    // Should this game count for this combination of numCards, mulligans, position, and
                                    // numDrawn?
                                    $switch: {
                                      branches: [
                                        // If the game didn't have this many mulligans, don't count it for anything.
                                        {
                                          case: { $lte: [{ $size: "$handsDrawn" }, "$$mulligans"] },
                                          then: NumberInt(0)
                                        },
                                        // If the position/numCards combination covers a reliably identifiable set of
                                        // cards, and the numDrawn index matches, then count this game here.
                                        {
                                          case: {
                                            $and: [
                                              // A reliably identifiable set of cards has different cards at and before
                                              // its start position.
                                              {
                                                $ne: [
                                                  { $arrayElemAt: ["$$deckArray", "$$position"] },
                                                  {
                                                    $arrayElemAt: [
                                                      "$$deckArray",
                                                      { $subtract: ["$$position", NumberInt(1)] }
                                                    ]
                                                  }
                                                ]
                                              },{
                                                // A reliably identifiable set of cards has different cards at and after
                                                // its end position.
                                                $ne: [
                                                  {
                                                    $arrayElemAt: [
                                                      "$$deckArray",
                                                      { $add: ["$$position", "$$numCards", NumberInt(-1)] }
                                                    ]
                                                  },{
                                                    $arrayElemAt: [
                                                      "$$deckArray",
                                                      { $add: ["$$position", "$$numCards"] }
                                                    ]
                                                  }
                                                ]
                                              },{
                                                // Was the right number of the right cards drawn?
                                                $eq: [
                                                  "$$numDrawn",
                                                  // Get the hand for this mulligan, and count how many cards drawn
                                                  // match the numCards cards at and after this position.
                                                  {
                                                    $size: {
                                                      $filter: {
                                                        input: { $arrayElemAt: ["$handsDrawn", "$$mulligans"] },
                                                        as: "card",
                                                        cond: {
                                                          $in: [
                                                            "$$card",
                                                            { $slice: ["$$deckArray", "$$position", "$$numCards"] }
                                                          ]
                                                        }
                                                      }
                                                    }
                                                  }
                                                ]
                                              }
                                            ]
                                          },
                                          then: NumberInt(1)
                                        }
                                      ],
                                      // This game does not match these index values, so don't count it here.
                                      default: NumberInt(0)
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
          $cond: {
            if: { $ne: ["$stats.numCards", NumberInt(0)] },
            then: {
              numCards: "$stats.numCards",
              week: "$week"
            },
            else: {
              week: "$week"
            }
          }
        },
        date: { $max: "$date" },
        // stats.distribution from the previous projection is a 3 dimensional array, following the output structure
        // specified above this pipeline.
        distributions: { $push: "$stats.distribution" }
      }
    },

    // Stage 12
    {
      // The process of merging the accumulated distributions and the existing stats is the same, might as well do the
      // lookup first and combine the merge steps.
      $lookup: {
        from: "position_stats",
        localField: "_id",
        foreignField: "_id",
        as: "stats"
      }
    },

    // Stage 13
    {
      $project: {
        date: NumberInt(1),
        // distribution will be a 3 dimensional array, merged by summing the inner elements of each input distribution.
        distribution: {
          $reduce: {
            input: "$distributions",
            initialValue: {
              $cond: {
                if: { $ne: [{ $size: "$stats.distribution" }, NumberInt(0)] },
                then: {
                  $arrayElemAt: ["$stats.distribution", NumberInt(0)]
                },
                // If no existing stats available, start with all zeros with the same size structure as distributions.
                else: {
                  $map: {
                    input: { $arrayElemAt: ["$distributions", NumberInt(0)] },
                    as: "middleArray",
                    in: {
                      $map: {
                        input: "$$middleArray",
                        as: "innerArray",
                        in: {
                          $map: {
                            input: "$$innerArray",
                            as: "innerValue",
                            in: NumberInt(0)
                          }
                        }
                      }
                    }
                  }
                }
              }
            },
            in: {
              $map: {
                // Pair up same-mulligans 2 dimensional middle arrays.
                input: { $zip: { inputs: ["$$value", "$$this"] } },
                as: "mulliganStats",
                in: {
                  $map: {
                    // Pair up same-position 1 dimensional middle arrays.
                    input: {
                      $zip: {
                        inputs: [
                          { $arrayElemAt: ["$$mulliganStats", NumberInt(0)] },
                          { $arrayElemAt: ["$$mulliganStats", NumberInt(1)] }
                        ]
                      }
                    },
                    as: "positionStats",
                    in: {
                      $map: {
                        // Pair up the actual values that have all three indices matching.
                        input: {
                          $zip: {
                            inputs: [
                              { $arrayElemAt: ["$$positionStats", NumberInt(0)] },
                              { $arrayElemAt: ["$$positionStats", NumberInt(1)] }
                            ]
                          }
                        },
                        as: "countStats",
                        // Finally, add the paired-up values.
                        in: { $sum: "$$countStats" }
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

    // Stage 14
    {
      $out: "position_stats"
    },

  ]

  // Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

);
