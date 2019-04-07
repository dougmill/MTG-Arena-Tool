<?php

require_once(join(DIRECTORY_SEPARATOR,[__DIR__,'..','api.php']));
$api = Api::getInstance();

// Want to process matches a) newer than any processed before, and b) old enough that any reasonable variation in clock
// time on the player's computer will not make a match be missed.
date_default_timezone_set('UTC');
$last_date_query = [
    [
        '$group' => [
            '_id' => NULL,
            'date' => [
                '$max' => '$date'
            ]
        ]
    ]
];
// This is the same format as JavaScript uses for serializing to json, which is what's stored in the database. It's a
// string, but this format is designed so that string sort order is the same as date order. Look up ISO 8601 if you want
// to know more. Limit to matches at least 10 minutes old.
$recent_date = date('Y\-m\-d\TH\:i\:s\.\0\0\0\Z', time() - 600);
$mongo_matches = (new MongoDB\Client)->tracker->matches;

$mongo_lands = (new MongoDB\Client)->tracker->land_stats;;
$cursor = $mongo_lands->aggregate($last_date_query);
$date_recorded = "2019-01-01T00:00:00.000Z";
foreach ($cursor as $document) {
    $date_recorded = $document['date'];
}

$land_pipeline = [
    [
        '$match' => [
            '$and' => [
                [
                    'date' => [
                        '$gt' => $date_recorded
                    ]
                ], [
                    'date' => [
                        '$lt' => $recent_date
                    ]
                ]
            ]
        ]
    ], [
        '$match' => [
            'gameStats.0.libraryLands' => [
                '$exists' => TRUE
            ],
            // When Tool is run from source, it may contain experimental or in-development changes that have not been
            // fully tested and reviewed to ensure they do not produce inaccurate or incorrectly formatted data.
            'toolRunFromSource' => [
                '$ne' => TRUE
            ]
        ]
    ], [
        '$sort' => [
            'date' => 1
        ]
    ], [
        '$limit' => 2000
    ], [
        // By this point, we have the (at most) 2000 earliest matches that both are in the target date range and have
        // shuffler data. Now trim out the non relevant fields.
        '$project' => [
            '_id' => 0,
            'date' => 1,
            'bestOf' => 1,
            'shuffling' => [
                '$cond' => [
                    [
                        // Smoothed shuffling was added for the Play queue only, in the February update.
                        '$and' => [
                            [
                                '$eq' => [
                                    '$eventId', 'Play'
                                ]
                            ], [
                                '$gt' => [
                                    '$date', '2019-02-14T15:00:00.000Z'
                                ]
                            ]
                        ]
                    ], 'smoothed', 'standard'
                ]
            ],
            'gameStats' => 1,
            'toolVersion' => 1
        ]
    ], [
        // gameStats is where all the shuffler data is.
        '$unwind' => [
            'path' => '$gameStats',
            'includeArrayIndex' => 'gameIndex'
        ]
    ], [
        // To limit how much space the aggregated data takes up, restrict the range of games it considers. Games are
        // included that have 40 to 42 cards with 14 to 20 lands, or 60 to 62 cards with 18 to 28 lands, and have at
        // most 3 mulligans. This captures an overwhelming majority of games while keeping my estimate for total space
        // less than 100 MB.
        '$match' => [
            'gameStats.handLands' => [
                '$exists' => TRUE
            ],
            '$expr' => [
                '$lte' => [
                    [
                        // handLands is an array with one entry per hand drawn, so size <= 4 fits 3 or fewer mulligans
                        '$size' => '$gameStats.handLands'
                    ], 4
                ]
            ],
            '$and' => [
                [
                    // There was a bug where cards stolen by Thief of Sanity were recorded incorrectly. This filters
                    // out affected games.
                    'gameStats.shuffledOrder' => [ '$ne' => 3 ]
                ], [
                    // There was a brief period where the latest build (a prerelease) might in some circumstances not
                    // stop recording cards when it got to an unknown one. Filter out any affected games.
                    'gameStats.shuffledOrder' => [ '$ne' => NULL ]
                ], [
                    // If we don't have any information about the library at all (concede during mulligan?) then filter
                    // the game out.
                    'gameStats.shuffledOrder' => [ '$ne' => [] ]
                ], [
                    '$or' => [
                        [
                            'gameStats.deckSize' => [
                                '$in' => [
                                    40, 41, 42
                                ]
                            ],
                            '$and' => [
                                [
                                    'gameStats.landsInDeck' => [
                                        '$gte' => 14
                                    ]
                                ], [
                                    'gameStats.landsInDeck' => [
                                        '$lte' => 20
                                    ]
                                ]
                            ]
                        ], [
                            'gameStats.deckSize' => [
                                '$in' => [
                                    60, 61, 62
                                ]
                            ],
                            '$and' => [
                                [
                                    'gameStats.landsInDeck' => [
                                        '$gte' => 18
                                    ]
                                ], [
                                    'gameStats.landsInDeck' => [
                                        '$lte' => 28
                                    ]
                                ]
                            ]
                        ]
                    ]
                ], [
                    // The February update broke Tool's sideboard tracking. The Tool release that fixed it also added
                    // the toolVersion field. Filter out any game that is a) after the Feb update, b) potentially
                    // sideboarded, and c) before the sideboard tracking fix. Inverting those conditions, this keeps
                    // games that are before the Feb update, the first game in a match, or after the sideboard tracking
                    // fix.
                    '$or' => [
                        [
                            'gameIndex' => 0
                        ], [
                            'toolVersion' => [ '$exists' => TRUE ]
                        ], [
                            'date' => [ '$lt' => '2019-02-14T15:00:00.000Z' ]
                        ]
                    ]
                ]
            ]
        ]
    ], [
        '$group' => [
            '_id' => [
                'deckSize' => '$gameStats.deckSize',
                'landsInDeck' => '$gameStats.landsInDeck',
                'librarySize' => '$gameStats.librarySize',
                'landsInLibrary' => '$gameStats.landsInLibrary',
                'bestOf' => '$bestOf',
                'shuffling' => '$shuffling'
            ],
            'date' => [
                '$max' => '$date'
            ],
            // libraryLands is an array of numbers for how many lands were in the top N cards of the library. For
            // example, if the library contained [land, nonland, nonland, land, rest unknown], then libraryLands would
            // have value [1, 1, 1, 2].
            'libraryLandSets' => [
                '$push' => '$gameStats.libraryLands'
            ]
        ]
    ], [
        '$project' => [
            'date' => 1,
            // distribution will be a 3 dimensional array, structured as:
            //      [
            //          // First index is position in the library.
            //          [
            //              // Second index is number of lands at or above that position.
            //              [
            //                  <Number of games known to match the first two indices>,
            //                  <Number of games where the card at this position is the first unknown card, and the game
            //                      otherwise matches the first two indices>
            //              ]
            //          ]
            //      ]
            'distribution' => [
                '$map' => [
                    // Outer index, 0-based position in library
                    'input' => [
                        '$range' => [
                            0, '$_id.librarySize'
                        ]
                    ],
                    'as' => 'position',
                    'in' => [
                        '$map' => [
                            // Inner index, number of lands at or before the current position. Cover every value from 0
                            // to the maximum possible number of lands for this position.
                            'input' => [
                                '$range' => [
                                    0, [
                                        '$cond' => [
                                            [
                                                '$lt' => [
                                                    '$$position', '$_id.landsInLibrary'
                                                ]
                                            ], [
                                                '$add' => [
                                                    '$$position', 2
                                                ]
                                            ], [
                                                '$add' => [
                                                    '$_id.landsInLibrary', 1
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ],
                            'as' => 'count',
                            'in' => [
                                [
                                    // First value, number of games known to match.
                                    '$size' => [
                                        '$filter' => [
                                            'input' => '$libraryLandSets',
                                            'as' => 'lands',
                                            'cond' => [
                                                '$eq' => [
                                                    [
                                                        '$arrayElemAt' => [
                                                            '$$lands', '$$position'
                                                        ]
                                                    ], '$$count'
                                                ]
                                            ]
                                        ]
                                    ]
                                ], [
                                    // Second value, number of games where the last known land count matches and there
                                    // is precisely one unknown card.
                                    '$size' => [
                                        '$filter' => [
                                            'input' => '$libraryLandSets',
                                            'as' => 'lands',
                                            'cond' => [
                                                '$and' => [
                                                    [
                                                        '$eq' => [
                                                            [
                                                                '$arrayElemAt' => [
                                                                    '$$lands', -1
                                                                ]
                                                            ], '$$count'
                                                        ]
                                                    ], [
                                                        // position is a 0-based index, the actual number of cards being
                                                        // considered is 1 higher, so this is checking if the number of
                                                        // known cards (size of land-counting array) is precisely 1 less
                                                        // than the number of cards being considered.
                                                        '$eq' => [
                                                            [
                                                                '$size' => '$$lands'
                                                            ], '$$position'
                                                        ]
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ], [
        // Need to merge with previously aggregated data.
        '$lookup' => [
            'from' => 'land_stats',
            'localField' => '_id',
            'foreignField' => '_id',
            'as' => 'stats'
        ]
    ], [
        '$project' => [
            'date' => 1,
            'distribution' => [
                '$cond' => [
                    // If there is no previously aggregated data for this group, then save some time and skip the merge.
                    [
                        '$eq' => [
                            [
                                '$size' => '$stats'
                            ], 0
                        ]
                    ], '$distribution', [
                        '$map' => [
                            // Pair up same-position 2 dimensional middle arrays.
                            'input' => [
                                '$zip' => [
                                    'inputs' => [
                                        '$distribution', [
                                            // There can only ever be one matching doc to merge with, so extract it.
                                            '$arrayElemAt' => [
                                                '$stats.distribution', 0
                                            ]
                                        ]
                                    ]
                                ]
                            ],
                            'as' => 'positionStats',
                            'in' => [
                                '$map' => [
                                    // Pair up same-land-count 1 dimensional inner arrays.
                                    'input' => [
                                        '$zip' => [
                                            'inputs' => [
                                                [
                                                    '$arrayElemAt' => [
                                                        '$$positionStats', 0
                                                    ]
                                                ], [
                                                    '$arrayElemAt' => [
                                                        '$$positionStats', 1
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ],
                                    'as' => 'countStats',
                                    'in' => [
                                        '$map' => [
                                            // Pair up the actual values that have all three indices matching, one value
                                            // from new data, one from old.
                                            'input' => [
                                                '$zip' => [
                                                    'inputs' => [
                                                        [
                                                            '$arrayElemAt' => [
                                                                '$$countStats', 0
                                                            ]
                                                        ], [
                                                            '$arrayElemAt' => [
                                                                '$$countStats', 1
                                                            ]
                                                        ]
                                                    ]
                                                ]
                                            ],
                                            'as' => 'stats',
                                            // Finally, add the paired-up values.
                                            'in' => [
                                                '$sum' => '$$stats'
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ]
];

// I originally had an $out stage at the end, but that would often drop existing data for rare combinations because the
// new 2000 matches didn't have a new game to add with that combination. So, do batched upserts instead.
$updatesBatch = [];
$ready = 0;
$cursor = $mongo_matches->aggregate($land_pipeline, [ 'allowDiskUse' => TRUE ]);
foreach ($cursor as $document) {
    $updatesBatch[] = [ 'replaceOne' => [[ '_id' => $document['_id'] ], $document, [ 'upsert' => TRUE ]] ];
    $ready++;
    if ($ready === 100) {
        $mongo_lands->bulkWrite($updatesBatch, [ 'ordered' => FALSE ]);
        $updatesBatch = [];
        $ready = 0;
    }
}
if ($ready !== 0) {
    $mongo_lands->bulkWrite($updatesBatch, [ 'ordered' => FALSE ]);
}

$mongo_hands = (new MongoDB\Client)->tracker->hand_stats;
$cursor = $mongo_hands->aggregate($last_date_query);
$date_recorded = "2019-01-01T00:00:00.000Z";
foreach ($cursor as $document) {
    $date_recorded = $document['date'];
}

$hand_pipeline = [
    [
        '$match' => [
            '$and' => [
                [
                    'date' => [
                        '$gt' => $date_recorded
                    ]
                ], [
                    'date' => [
                        '$lt' => $recent_date
                    ]
                ]
            ]
        ]
    ], [
        '$match' => [
            'gameStats.0.libraryLands' => [
                '$exists' => TRUE
            ],
            // When Tool is run from source, it may contain experimental or in-development changes that have not been
            // fully tested and reviewed to ensure they do not produce inaccurate or incorrectly formatted data.
            'toolRunFromSource' => [
                '$ne' => TRUE
            ]
        ]
    ], [
        '$sort' => [
            'date' => 1
        ]
    ], [
        '$limit' => 2000
    ], [
        // By this point, we have the (at most) 2000 earliest matches that both are in the target date range and have
        // shuffler data. Now trim out the non relevant fields.
        '$project' => [
            '_id' => 0,
            'date' => 1,
            'bestOf' => 1,
            'gameStats' => 1,
            'toolVersion' => 1
        ]
    ], [
        // gameStats is where all the shuffler data is.
        '$unwind' => [
            'path' => '$gameStats',
            'includeArrayIndex' => 'gameIndex'
        ]
    ], [
        // To limit how much space the aggregated data takes up, restrict the range of games it considers. Games are
        // included that have 40 to 50 cards with 14 to 25 lands, or 60 to 70 cards with 18 to 32 lands, and any
        // number of mulligans. This captures an overwhelming majority of games while keeping my estimate for total
        // space reasonable. These ranges are larger than those for lands in the library because there is less data to
        // store per combination due to only looking at opening hands and mulligans.
        '$match' => [
            'gameStats.shuffledOrder' => [ '$ne' => [] ],
            '$and' => [
                [
                    // The February update broke Tool's sideboard tracking. The Tool release that fixed it also added
                    // the toolVersion field. Filter out any game that is a) after the Feb update, b) potentially
                    // sideboarded, and c) before the sideboard tracking fix. Inverting those conditions, this keeps
                    // games that are before the Feb update, the first game in a match, or after the sideboard tracking
                    // fix.
                    '$or' => [
                        [
                            'gameIndex' => 0
                        ], [
                            'toolVersion' => [ '$exists' => TRUE ]
                        ], [
                            'date' => [ '$lt' => '2019-02-14T15:00:00.000Z' ]
                        ]
                    ]
                ], [
                    '$or' => [
                        [
                            '$and' => [
                                [
                                    'gameStats.deckSize' => [
                                        '$gte' => 40
                                    ]
                                ], [
                                    'gameStats.deckSize' => [
                                        '$lte' => 50
                                    ]
                                ], [
                                    'gameStats.landsInDeck' => [
                                        '$gte' => 14
                                    ]
                                ], [
                                    'gameStats.landsInDeck' => [
                                        '$lte' => 25
                                    ]
                                ]
                            ]
                        ], [
                            '$and' => [
                                [
                                    'gameStats.deckSize' => [
                                        '$gte' => 60
                                    ]
                                ], [
                                    'gameStats.deckSize' => [
                                        '$lte' => 70
                                    ]
                                ], [
                                    'gameStats.landsInDeck' => [
                                        '$gte' => 18
                                    ]
                                ], [
                                    'gameStats.landsInDeck' => [
                                        '$lte' => 32
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ], [
        '$addFields' => [
            // "Smooth" shuffling was added in the February update, but even before then the opening hand (before
            // mulligans) of Bo1 games had a slightly reduced version of it working. Mark shuffling type as:
            //      Bo1 games after the Feb update: All hands smoothed, including mulligans.
            //      Bo1 games before the Feb update: First hand smoothed, mulligans not smoothed.
            //      Bo3 games: All hands standard shuffling.
            'shuffling' => [
                '$switch' => [
                    'branches' => [
                        [
                            'case' => [ '$and' => [[ '$eq' => ['$bestOf', 1] ], [ '$gt' => ['$date', '2019-02-14T15:00:00.000Z'] ]] ],
                            'then' => ['smoothed']
                        ], [
                            'case' => [ '$eq' => ['$bestOf', 1] ],
                            'then' => ['smoothed', 'standard']
                        ]
                    ],
                    'default' => ['standard']
                ]
            ]
        ]
    ], [
        // For Bo1 games before the Feb update, some hands will go in each type of shuffling, so split that up.
        '$unwind' => [
            'path' => '$shuffling'
        ]
    ], [
        '$group' => [
            '_id' => [
                'deckSize' => '$gameStats.deckSize',
                'landsInDeck' => '$gameStats.landsInDeck',
                'bestOf' => '$bestOf',
                'shuffling' => '$shuffling'
            ],
            'date' => [
                '$max' => '$date'
            ],
            // handLands in the input is a 1 dimensional array, with each value being the number of lands in a hand
            // drawn in the game, in the same order as the hands were drawn.
            'handLands' => [
                '$push' => [
                    // Games that are either Bo3 or after the Feb update use the same shuffling for all hands drawn,
                    // including mulligans. For Bo1 before the update, if shuffling is smoothed then count the first
                    // hand, otherwise count the mulligan hands.
                    '$switch' => [
                        'branches' => [
                            [
                                'case' => [ '$or' => [[ '$eq' => ['$bestOf', 3] ], [ '$gt' => ['$date', '2019-02-14T15:00:00.000Z'] ]] ],
                                'then' => '$gameStats.handLands'
                            ], [
                                'case' => [ '$eq' => ['$shuffling', 'smoothed'] ],
                                'then' => [[ '$arrayElemAt' => ['$gameStats.handLands', 0] ]]
                            ]
                        ],
                        // Need to have something in the index 0 spot to keep the rest of the logic working, fill it
                        // with a dummy value that won't be matched with anything.
                        'default' => [ '$concatArrays' => [[-1], [ '$slice' => ['$gameStats.handLands', 1, 7] ]] ]
                    ]
                ]
            ]
        ]
    ], [
        '$project' => [
            'date' => 1,
            // Distribution will be a 2 dimensional array, structured as:
            //      [
            //          // First index is number of mulligans.
            //          [
            //              // Second index is number of lands in the drawn hand.
            //              <Number of games that match the indices>
            //          ]
            //      ]
            'distribution' => [
                '$map' => [
                    // Size of the hand at each value of the outer index
                    'input' => [
                        '$range' => [
                            7, 0, -1
                        ]
                    ],
                    'as' => 'handSize',
                    'in' => [
                        '$map' => [
                            // Inner index, number of lands in the hand. Cover every value from 0 lands to all lands
                            'input' => [
                                '$range' => [
                                    0, [
                                        '$add' => [
                                            '$$handSize', 1
                                        ]
                                    ]
                                ]
                            ],
                            'as' => 'count',
                            'in' => [
                                // Count number of games that match the indices.
                                '$size' => [
                                    '$filter' => [
                                        'input' => '$handLands',
                                        'as' => 'hand',
                                        'cond' => [
                                            '$eq' => [
                                                [
                                                    '$arrayElemAt' => [
                                                        '$$hand', [
                                                            '$subtract' => [
                                                                7, '$$handSize'
                                                            ]
                                                        ]
                                                    ]
                                                ], '$$count'
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ], [
        // Need to merge with previously aggregated data.
        '$lookup' => [
            'from' => 'hand_stats',
            'localField' => '_id',
            'foreignField' => '_id',
            'as' => 'stats'
        ]
    ], [
        '$project' => [
            'date' => 1,
            'distribution' => [
                '$cond' => [
                    // If there is no previously aggregated data for this group, then save some time and skip the merge.
                    [
                        '$eq' => [
                            [
                                '$size' => '$stats'
                            ], 0
                        ]
                    ], '$distribution', [
                        '$map' => [
                            // Pair up same-mulligans 1 dimensional inner arrays.
                            'input' => [
                                '$zip' => [
                                    'inputs' => [
                                        '$distribution', [
                                            // There can only ever be one matching doc to merge with, so extract it.
                                            '$arrayElemAt' => [
                                                '$stats.distribution', 0
                                            ]
                                        ]
                                    ]
                                ]
                            ],
                            'as' => 'handSizeStats',
                            'in' => [
                                '$map' => [
                                    // Pair up the actual values that have both indices matching, one value from new
                                    // data, one from old.
                                    'input' => [
                                        '$zip' => [
                                            'inputs' => [
                                                [
                                                    '$arrayElemAt' => [
                                                        '$$handSizeStats', 0
                                                    ]
                                                ], [
                                                    '$arrayElemAt' => [
                                                        '$$handSizeStats', 1
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ],
                                    'as' => 'countStats',
                                    // Finally, add the paired-up values.
                                    'in' => [
                                        '$sum' => '$$countStats'
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ]
];

// I originally had an $out stage at the end, but that would often drop existing data for rare combinations because the
// new 2000 matches didn't have a new game to add with that combination. So, do batched upserts instead.
$updatesBatch = [];
$ready = 0;
$cursor = $mongo_matches->aggregate($hand_pipeline, [ 'allowDiskUse' => TRUE ]);
foreach ($cursor as $document) {
    $updatesBatch[] = [ 'replaceOne' => [[ '_id' => $document['_id'] ], $document, [ 'upsert' => TRUE ]] ];
    $ready++;
    if ($ready === 100) {
        $mongo_hands->bulkWrite($updatesBatch, [ 'ordered' => FALSE ]);
        $updatesBatch = [];
        $ready = 0;
    }
}
if ($ready !== 0) {
    $mongo_hands->bulkWrite($updatesBatch, [ 'ordered' => FALSE ]);
}

$mongo_cards = (new MongoDB\Client)->tracker->card_stats;
$cursor = $mongo_cards->aggregate($last_date_query);
$date_recorded = "2019-01-01T00:00:00.000Z";
foreach ($cursor as $document) {
    $date_recorded = $document['date'];
}

$card_pipeline = [
    [
        '$match' => [
            '$and' => [
                [
                    'date' => [
                        '$gt' => $date_recorded
                    ]
                ], [
                    'date' => [
                        '$lt' => $recent_date
                    ]
                ]
            ]
        ]
    ], [
        '$match' => [
            'gameStats.0.libraryLands' => [
                '$exists' => TRUE
            ],
            // When Tool is run from source, it may contain experimental or in-development changes that have not been
            // fully tested and reviewed to ensure they do not produce inaccurate or incorrectly formatted data.
            'toolRunFromSource' => [
                '$ne' => TRUE
            ]
        ]
    ], [
        '$sort' => [
            'date' => 1
        ]
    ], [
        '$limit' => 2000
    ], [
        // By this point, we have the (at most) 2000 earliest matches that both are in the target date range and have
        // shuffler data. Now trim out the non relevant fields.
        '$project' => [
            '_id' => 0,
            'date' => 1,
            'bestOf' => 1,
            'shuffling' => [
                '$cond' => [
                    [
                        // Smoothed shuffling was added for the Play queue only, in the February update.
                        '$and' => [
                            [
                                '$eq' => [
                                    '$eventId', 'Play'
                                ]
                            ], [
                                '$gt' => [
                                    '$date', '2019-02-14T15:00:00.000Z'
                                ]
                            ]
                        ]
                    ], 'smoothed', 'standard'
                ]
            ],
            'gameStats' => 1,
            'toolVersion' => 1
        ]
    ], [
        // gameStats is where all the shuffler data is.
        '$unwind' => [
            'path' => '$gameStats',
            'includeArrayIndex' => 'gameIndex'
        ]
    ], [
        // To limit how much space the aggregated data takes up, restrict the range of games it considers. Games are
        // included that have 40 to 42 cards with 14 to 20 lands, or 60 to 62 cards with 18 to 28 lands, and have at
        // most 3 mulligans. This captures an overwhelming majority of games while keeping my estimate for total space
        // less than 100 MB.
        '$match' => [
            'gameStats.handLands' => [
                '$exists' => TRUE
            ],
            '$expr' => [
                '$lte' => [
                    [
                        // handLands is an array with one entry per hand drawn, so size <= 4 fits 3 or fewer mulligans
                        '$size' => '$gameStats.handLands'
                    ], 4
                ]
            ],
            '$and' => [
                [
                    'gameStats.shuffledOrder' => [ '$exists' => TRUE ]
                ], [
                    // There was a bug where cards stolen by Thief of Sanity were recorded incorrectly. This filters
                    // out affected games.
                    'gameStats.shuffledOrder' => [ '$ne' => 3 ]
                ], [
                    // There was a brief period where the latest build (a prerelease) might in some circumstances not
                    // stop recording cards when it got to an unknown one. Filter out any affected games.
                    'gameStats.shuffledOrder' => [ '$ne' => NULL ]
                ], [
                    // If we don't have any information about the library at all (concede during mulligan?) then filter
                    // the game out.
                    'gameStats.shuffledOrder' => [ '$ne' => [] ]
                ], [
                    '$or' => [
                        [
                            'gameStats.deckSize' => [
                                '$in' => [
                                    40, 41, 42
                                ]
                            ],
                            '$and' => [
                                [
                                    'gameStats.landsInDeck' => [
                                        '$gte' => 14
                                    ]
                                ], [
                                    'gameStats.landsInDeck' => [
                                        '$lte' => 20
                                    ]
                                ]
                            ]
                        ], [
                            'gameStats.deckSize' => [
                                '$in' => [
                                    60, 61, 62
                                ]
                            ],
                            '$and' => [
                                [
                                    'gameStats.landsInDeck' => [
                                        '$gte' => 18
                                    ]
                                ], [
                                    'gameStats.landsInDeck' => [
                                        '$lte' => 28
                                    ]
                                ]
                            ]
                        ]
                    ]
                ], [
                    // The February update broke Tool's sideboard tracking. The Tool release that fixed it also added
                    // the toolVersion field. Filter out any game that is a) after the Feb update, b) potentially
                    // sideboarded, and c) before the sideboard tracking fix. Inverting those conditions, this keeps
                    // games that are before the Feb update, the first game in a match, or after the sideboard tracking
                    // fix.
                    '$or' => [
                        [
                            'gameIndex' => 0
                        ], [
                            'toolVersion' => [ '$exists' => TRUE ]
                        ], [
                            'date' => [ '$lt' => '2019-02-14T15:00:00.000Z' ]
                        ]
                    ]
                ]
            ]
        ]
    ], [
        '$project' => [
            'date' => 1,
            'deckSize' => '$gameStats.deckSize',
            'bestOf' => 1,
            'shuffling' => 1,
            // Card positions per unique card are not stored in a way that implicitly includes the number of known cards
            // in the library, so get that number before dropping the fields that have it.
            'cardsKnown' => [
                '$size' => '$gameStats.shuffledOrder'
            ],
            // multiCardPositions is an object structured as:
            //      {
            //          <number of copies of a card, 2 to 4>: {
            //              <card id>: [
            //                  <each known 1-based position in the deck of a copy of this card>
            //              ]
            //          }
            //      }
            // This needs some splitting out and transforming before grouping can be done. To illustrate by example,
            // this transforms this:
            //      {
            //          2: { 4567: [3, 7], 6789: [] },
            //          3: {},
            //          4: {}
            //      }
            // into this:
            //      [{ k: 2, v: { 4567: [3, 7], 6789: [] } },
            //       { k: 3, v: {} },
            //       { k: 4, v: {} }]
            'positionSets' => [
                '$objectToArray' => '$gameStats.multiCardPositions'
            ]
        ]
    ], [
        // Treat each number of copies separately.
        '$unwind' => '$positionSets'
    ], [
        // If v is an empty object, then this game didn't have any cards with this number of copies so there's nothing
        // to analyze. Have to cast because otherwise mongo would be checking for empty *array*, and it's not an array.
        '$match' => [
            'positionSets.v' => [
                '$ne' => (object)[]
            ]
        ]
    ], [
        // Next step of transform.
        '$project' => [
            'date' => 1,
            'deckSize' => 1,
            'bestOf' => 1,
            'shuffling' => 1,
            // Need the number of copies for grouping later.
            'copies' => [
                '$toInt' => '$positionSets.k'
            ],
            'cardsKnown' => 1,
            'positionSets' => [
                '$let' => [
                    'vars' => [
                        'positionsObjects' => [
                            '$map' => [
                                // Continuing the example, this transforms { k: 2, v: { 4567: [3, 7], 6789: [] } } into
                                // [{ k: 4567, v: [3, 7] }, { k: 6789, v: [] }]
                                'input' => [
                                    '$objectToArray' => '$positionSets.v'
                                ],
                                'as' => 'positions',
                                // This discards the card id and extracts the positions, transforming the example into:
                                // [{ type: "all", positions: [3, 7] }, { type: "all", positions: [] }]
                                'in' => [
                                    'type' => 'all',
                                    'positions' => '$$positions.v'
                                ]
                            ]
                        ]
                    ],
                    // Due to the sets of cards being in the same game, their positions are not independent. I want to
                    // have them all available to view, but for proper statistical analysis I want to have just one
                    // card set in the group per game. Take the array from above, and append its first element with a
                    // different "type" value. The example is now:
                    // [{ type: "all", positions: [3, 7] },
                    //  { type: "all", positions: [] },
                    //  { type: "first", positions: [3, 7] }]
                    'in' => [
                        '$concatArrays' => [
                            '$$positionsObjects', [
                                [
                                    'type' => 'first',
                                    'positions' => [
                                        '$arrayElemAt' => [
                                            '$$positionsObjects.positions', 0
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ], [
        // Now separate out each card set so they can be grouped. One entry from the example is now
        // { type: "all", positions: [3, 7] }
        '$unwind' => '$positionSets'
    ], [
        '$group' => [
            '_id' => [
                'deckSize' => '$deckSize',
                'bestOf' => '$bestOf',
                'shuffling' => '$shuffling',
                'copies' => '$copies',
                'type' => '$positionSets.type'
            ],
            'date' => [
                '$max' => '$date'
            ],
            // One doc's result for this accumulated field from the example is:
            // [{ positions: [3, 7], known: 10 }, { positions: [], known 10 }]
            'positionSets' => [
                '$push' => [
                    'positions' => '$positionSets.positions',
                    'known' => '$cardsKnown'
                ]
            ]
        ]
    ], [
        '$project' => [
            'date' => 1,
            // Now we're ready to count things up. Distribution will be a 3 dimensional array, structured as:
            //      [
            //          // First index is position in the deck.
            //          [
            //              // Second index is number of copies at or above that position.
            //              [
            //                  <Number of games known to match the first two indices>,
            //                  <Number of games where the card at this position is the first unknown card, and the game
            //                      otherwise matches the first two indices>
            //              ]
            //          ]
            //      ]
            'distribution' => [
                '$map' => [
                    // Outer index offset by 1 because it makes some later logic easier, 1-based position in deck
                    'input' => [
                        '$range' => [
                            1, [
                                '$add' => [
                                    '$_id.deckSize', 1
                                ]
                            ]
                        ]
                    ],
                    'as' => 'position',
                    'in' => [
                        '$map' => [
                            // Inner index, number of copies at or before the current position. Cover every value from 0
                            // to the number of copies in the deck.
                            'input' => [
                                '$range' => [
                                    0, [
                                        '$add' => [
                                            '$_id.copies', 1
                                        ]
                                    ]
                                ]
                            ],
                            'as' => 'copies',
                            'in' => [
                                '$let' => [
                                    'vars' => [
                                        // matches will be an array of entries that might fit the indices.
                                        'matches' => [
                                            '$filter' => [
                                                // positionSets example is:
                                                // [{ positions: [3, 7], known: 10 }, { positions: [], known 10 }]
                                                'input' => '$positionSets',
                                                // positions example is { positions: [3, 7], known: 10 }
                                                'as' => 'positions',
                                                // Count how many recorded position numbers are at or before the current
                                                // position index. Keep only entries that match the "copies" index.
                                                // Note that both the position index in this map and the card positions
                                                // in the input are 1-based - the top card is at position 1, not 0.
                                                'cond' => [
                                                    '$eq' => [
                                                        '$$copies', [
                                                            '$size' => [
                                                                '$filter' => [
                                                                    'input' => '$$positions.positions',
                                                                    'as' => 'cardPosition',
                                                                    'cond' => [
                                                                        '$lte' => [
                                                                            '$$cardPosition', '$$position'
                                                                        ]
                                                                    ]
                                                                ]
                                                            ]
                                                        ]
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ],
                                    'in' => [
                                        // Out of the games that fit the indices, first count the ones where every
                                        // card up to and including the current position is known.
                                        [
                                            '$size' => [
                                                '$filter' => [
                                                    'input' => '$$matches.known',
                                                    'as' => 'known',
                                                    'cond' => [
                                                        // position is 1-based, so if they're equal then every card up
                                                        // to the current position is known but none past it.
                                                        '$lte' => [
                                                            '$$position', '$$known'
                                                        ]
                                                    ]
                                                ]
                                            ]
                                        // Then count the games where the card at the current position is the first
                                        // unknown card.
                                        ], [
                                            '$size' => [
                                                '$filter' => [
                                                    'input' => '$$matches.known',
                                                    'as' => 'known',
                                                    'cond' => [
                                                        // position is 1-based, so this is checking if it's exactly 1
                                                        // card past the last known card.
                                                        '$eq' => [
                                                            '$$position', [
                                                                '$add' => [
                                                                    '$$known', 1
                                                                ]
                                                            ]
                                                        ]
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ], [
        // Need to merge with previously aggregated data.
        '$lookup' => [
            'from' => 'card_stats',
            'localField' => '_id',
            'foreignField' => '_id',
            'as' => 'stats'
        ]
    ], [
        '$project' => [
            'date' => 1,
            'distribution' => [
                '$cond' => [
                    // If there is no previously aggregated data for this group, then save some time and skip the merge.
                    [
                        '$eq' => [
                            [
                                '$size' => '$stats'
                            ], 0
                        ]
                    ], '$distribution', [
                        '$map' => [
                            // Pair up same-position 2 dimensional middle arrays.
                            'input' => [
                                '$zip' => [
                                    'inputs' => [
                                        '$distribution', [
                                            // There can only ever be one matching doc to merge with, so extract it.
                                            '$arrayElemAt' => [
                                                '$stats.distribution', 0
                                            ]
                                        ]
                                    ]
                                ]
                            ],
                            'as' => 'positionStats',
                            'in' => [
                                '$map' => [
                                    // Pair up same-copies-count 1 dimensional inner arrays.
                                    'input' => [
                                        '$zip' => [
                                            'inputs' => [
                                                [
                                                    '$arrayElemAt' => [
                                                        '$$positionStats', 0
                                                    ]
                                                ], [
                                                    '$arrayElemAt' => [
                                                        '$$positionStats', 1
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ],
                                    'as' => 'countStats',
                                    'in' => [
                                        '$map' => [
                                            // Pair up the actual values that have all three indices matching, one value
                                            // from new data, one from old.
                                            'input' => [
                                                '$zip' => [
                                                    'inputs' => [
                                                        [
                                                            '$arrayElemAt' => [
                                                                '$$countStats', 0
                                                            ]
                                                        ], [
                                                            '$arrayElemAt' => [
                                                                '$$countStats', 1
                                                            ]
                                                        ]
                                                    ]
                                                ]
                                            ],
                                            'as' => 'stats',
                                            // Finally, add the paired-up values.
                                            'in' => [
                                                '$sum' => '$$stats'
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ]
];

// I originally had an $out stage at the end, but that would often drop existing data for rare combinations because the
// new 2000 matches didn't have a new game to add with that combination. So, do batched upserts instead.
$updatesBatch = [];
$ready = 0;
$cursor = $mongo_matches->aggregate($card_pipeline, [ 'allowDiskUse' => TRUE ]);
foreach ($cursor as $document) {
    $updatesBatch[] = [ 'replaceOne' => [[ '_id' => $document['_id'] ], $document, [ 'upsert' => TRUE ]] ];
    $ready++;
    if ($ready === 100) {
        $mongo_cards->bulkWrite($updatesBatch, [ 'ordered' => FALSE ]);
        $updatesBatch = [];
        $ready = 0;
    }
}
if ($ready !== 0) {
    $mongo_cards->bulkWrite($updatesBatch, [ 'ordered' => FALSE ]);
}

$mongo_positions = (new MongoDB\Client)->tracker->position_stats;
$cursor = $mongo_positions->aggregate($last_date_query);
$date_recorded = "2019-01-01T00:00:00.000Z";
foreach ($cursor as $document) {
    $date_recorded = $document['date'];
}

// This aggregation is for analyzing position-dependent probability of a card being drawn in the opening hand. The cards
// at the very front of the decklist are considered, and separately those at the very back of the decklist. In both
// cases the amount of cards included is close to the most typical amounts of lands.
$position_pipeline = [
    [
        '$match' => [
            '$and' => [
                [
                    'date' => [
                        '$gt' => $date_recorded
                    ]
                ], [
                    'date' => [
                        '$lt' => $recent_date
                    ]
                ]
            ]
        ]
    ], [
        '$match' => [
            'gameStats.0.handsDrawn' => [
                '$exists' => TRUE
            ],
            // The Bo1 opening hand algorithm, even though it is aimed at lands, could still throw off the distributions
            // of position-in-decklist based stats. To prevent that, include only Bo3 games.
            'bestOf' => 3,
            // When Tool is run from source, it may contain experimental or in-development changes that have not been
            // fully tested and reviewed to ensure they do not produce inaccurate or incorrectly formatted data.
            'toolRunFromSource' => [
                '$ne' => TRUE
            ]
        ]
    ], [
        '$sort' => [
            'date' => 1
        ]
    ], [
        '$limit' => 2000
    ], [
        // By this point, we have the (at most) 2000 earliest matches that both are in the target date range and have
        // shuffler data. Now trim out the non relevant fields.
        '$project' => [
            '_id' => 0,
            'date' => 1,
            'gameStats' => 1,
            'toolVersion' => 1,
            'deckList' => '$playerDeck.mainDeck'
        ]
    ], [
        // gameStats is where all the shuffler data is.
        '$unwind' => [
            'path' => '$gameStats',
            'includeArrayIndex' => 'gameIndex'
        ]
    ], [
        '$project' => [
            'date' => 1,
            // It is possible for the decklist to change, even drastically, in sideboarding. The sideboarded decklist is
            // now included in the stats for games 2 and 3, replace the base decklist with it for those games.
            'deckList' => [
                '$cond' => [
                    [
                        '$eq' => [
                            '$gameIndex', 0
                        ]
                    ], '$deckList', '$gameStats.deck.mainDeck'
                ]
            ],
            'deckSize' => '$gameStats.deckSize',
            'handsDrawn' => '$gameStats.handsDrawn',
            'toolVersion' => 1
        ]
    ], [
        '$match' => [
            // The statistics for this aggregation depend heavily on the precise details of the decklist, which cannot
            // be reliably derived for sideboarded games from data recorded before the sideboarded decklist was added to
            // match records.
            'deckList' => [
                '$exists' => TRUE
            ],
            // The overwhelming majority of games have either 40 or 60 cards, to the point where there would be little
            // benefit from analyzing this aggregation for other deck sizes.
            'deckSize' => [
                '$in' => [
                    40, 60
                ]
            ],
            // The March update to Arena changed the format of most logged decklists, requiring a hotfix update to Tool
            // to handle it. Any games recorded by the previous version of Tool after Arena updated almost certainly
            // have missing or incorrect decklists, so filter them out.
            '$or' => [
                [
                    'toolVersion' => [
                        '$gte' => 131605
                    ]
                ], [
                    'date' => [
                        '$lt' => '2019-03-27T12:00:00.000Z'
                    ]
                ]
            ],
            // Sometimes, such as on concede during mulligan, the opening hand is not known and gets recorded as being
            // empty. Filter such games out to prevent them being counted as having 0 relevant cards in hand.
            'handsDrawn' => [ '$ne' => [] ]
        ]
    ], [
        '$project' => [
            'date' => 1,
            // In case WotC changed shuffling without mentioning it in release notes, keep separate records for each
            // Arena release. Tool began recording this data in January, when Arena version 0.11 was current.
            'update' => [
                '$switch' => [
                    'branches' => [
                        [
                            'case' => [
                                '$lt' => [
                                    '$date', '2019-02-14T14:00:00.000Z'
                                ]
                            ],
                            'then' => 11
                        ], [
                            'case' => [
                                '$lt' => [
                                    '$date', '2019-03-27T12:00:00.000Z'
                                ]
                            ],
                            'then' => 12
                        ]
                    ],
                    'default' => 13
                ]
            ],
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
            'stats' => [
                '$let' => [
                    'vars' => [
                        'bounds' => [
                            // First, find a suitable range of cards, if possible, at each of the front and the back.
                            // For 40 card decks, look for 15 to 18 cards. For 60 card decks, look for 22 to 25 cards.
                            // If there are multiple acceptable ranges to choose from, prefer the option closest to
                            // 17 or 24, breaking ties by choosing the smaller one. This reduce operation scans through
                            // the decklist, counting cards and moving the bounds as it goes, until it finds the best
                            // option and stops moving each bound.
                            '$reduce' => [
                                'input' => '$deckList',
                                'initialValue' => [
                                    // The index in the decklist of the card group being examined.
                                    'index' => 0,
                                    // The number of cards, accounting for quantity, that have already been checked.
                                    'cards' => 0,
                                    // The smallest acceptable number of cards.
                                    'minTarget' => [
                                        '$cond' => [
                                            [
                                                '$eq' => [
                                                    '$deckSize', 40
                                                ]
                                            ], 15, 22
                                        ]
                                    ],
                                    // The largest acceptable number of cards.
                                    'maxTarget' => [
                                        '$cond' => [
                                            [
                                                '$eq' => [
                                                    '$deckSize', 40
                                                ]
                                            ], 18, 25
                                        ]
                                    ],
                                    // Index where the front range ends, exclusive.
                                    'frontBound' => 0,
                                    // Index where the back range starts, inclusive.
                                    'backBound' => 0
                                ],
                                'in' => [
                                    // Track the index as we go.
                                    'index' => [
                                        '$add' => [
                                            '$$value.index', 1
                                        ]
                                    ],
                                    'cards' => [
                                        '$add' => [
                                            '$$value.cards', '$$this.quantity'
                                        ]
                                    ],
                                    'minTarget' => '$$value.minTarget',
                                    'maxTarget' => '$$value.maxTarget',
                                    'frontBound' => [
                                        '$switch' => [
                                            'branches' => [
                                                [
                                                    // First branch: Is it time to stop moving the front bound?
                                                    'case' => [
                                                        '$or' => [
                                                            [
                                                                // If not equal, then it's already been stopped.
                                                                '$ne' => [
                                                                    '$$value.index', '$$value.frontBound'
                                                                ]
                                                            ], [
                                                                // If moving it would go past the acceptable range, and
                                                                // it is currently in the acceptable range, then stop.
                                                                '$and' => [
                                                                    [
                                                                        '$gt' => [
                                                                            [
                                                                                '$add' => [
                                                                                    '$$value.cards', '$$this.quantity'
                                                                                ]
                                                                            ], '$$value.maxTarget'
                                                                        ]
                                                                    ], [
                                                                        '$gte' => [
                                                                            '$$value.cards', '$$value.minTarget'
                                                                        ]
                                                                    ]
                                                                ]
                                                            ], [
                                                                // If already in acceptable range, don't go to the max
                                                                // of the range from anywhere except the min - anywhere
                                                                // else is either the ideal spot or wins the tiebreaker.
                                                                '$and' => [
                                                                    [
                                                                        '$gt' => [
                                                                            '$$value.cards', '$$value.minTarget'
                                                                        ]
                                                                    ], [
                                                                        '$eq' => [
                                                                            [
                                                                                '$add' => [
                                                                                    '$$value.cards', '$$this.quantity'
                                                                                ]
                                                                            ], '$$value.maxTarget'
                                                                        ]
                                                                    ]
                                                                ]
                                                            ]
                                                        ]
                                                    ],
                                                    // Front bound either has already stopped or should be stopped now.
                                                    'then' => '$$value.frontBound'
                                                ], [
                                                    // Second branch: We're not in the acceptable range yet, otherwise
                                                    // the first branch would have been taken. Does this card skip over
                                                    // the acceptable range entirely?
                                                    'case' => [
                                                        '$gt' => [
                                                            [
                                                                '$add' => [
                                                                    '$$value.cards', '$$this.quantity'
                                                                ]
                                                            ], '$$value.maxTarget'
                                                        ]
                                                    ],
                                                    // Use an invalid value to mark that there's no acceptable range.
                                                    'then' => -1
                                                ]
                                            ],
                                            // Not time to stop, haven't gone too far, so keep it moving.
                                            'default' => [
                                                '$add' => [
                                                    '$$value.frontBound', 1
                                                ]
                                            ]
                                        ]
                                    ],
                                    'backBound' => [
                                        '$switch' => [
                                            'branches' => [
                                                [
                                                    // First branch: Is it time to stop moving the back bound?
                                                    'case' => [
                                                        '$or' => [
                                                            [
                                                                // If not equal, then it's already been stopped.
                                                                '$ne' => [
                                                                    '$$value.index', '$$value.backBound'
                                                                ]
                                                            ], [
                                                                // If moving it would go past the acceptable range, and
                                                                // it is currently in the acceptable range, then stop.
                                                                '$and' => [
                                                                    [
                                                                        '$gt' => [
                                                                            [
                                                                                '$add' => [
                                                                                    '$$value.cards', '$$this.quantity'
                                                                                ]
                                                                            ], [
                                                                                '$subtract' => [
                                                                                    '$deckSize', '$$value.minTarget'
                                                                                ]
                                                                            ]
                                                                        ]
                                                                    ], [
                                                                        '$gte' => [
                                                                            '$$value.cards', [
                                                                                '$subtract' => [
                                                                                    '$deckSize', '$$value.maxTarget'
                                                                                ]
                                                                            ]
                                                                        ]
                                                                    ]
                                                                ]
                                                            ], [
                                                                // If at exactly the max acceptable range, and this card
                                                                // would move to the min - the only value less preferred
                                                                // - then stop.
                                                                '$and' => [
                                                                    [
                                                                        '$eq' => [
                                                                            '$$value.cards', [
                                                                                '$subtract' => [
                                                                                    '$deckSize', '$$value.maxTarget'
                                                                                ]
                                                                            ]
                                                                        ]
                                                                    ], [
                                                                        '$eq' => [
                                                                            '$$this.quantity', 3
                                                                        ]
                                                                    ]
                                                                ]
                                                            ], [
                                                                // If in the acceptable range and not at the max of it,
                                                                // then stop.
                                                                '$gt' => [
                                                                    '$$value.cards', [
                                                                        '$subtract' => [
                                                                            '$deckSize', '$$value.maxTarget'
                                                                        ]
                                                                    ]
                                                                ]
                                                            ]
                                                        ]
                                                    ],
                                                    // Back bound either has already stopped or should be stopped now.
                                                    'then' => '$$value.backBound'
                                                ], [
                                                    // Second branch: We're not in the acceptable range yet, otherwise
                                                    // the first branch would have been taken. Does this card skip over
                                                    // the acceptable range entirely?
                                                    'case' => [
                                                        '$gt' => [
                                                            [
                                                                '$add' => [
                                                                    '$$value.cards', '$$this.quantity'
                                                                ]
                                                            ], [
                                                                '$subtract' => [
                                                                    '$deckSize', '$$value.minTarget'
                                                                ]
                                                            ]
                                                        ]
                                                    ],
                                                    // Use an invalid value to mark that there's no acceptable range.
                                                    'then' => -1
                                                ]
                                            ],
                                            // Not time to stop, haven't gone too far, so keep it moving.
                                            'default' => [
                                                '$add' => [
                                                    '$$value.backBound', 1
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ],
                        'quantities' => '$deckList.quantity',
                        // The decklist has card ids as strings, but in opening hands and most other places they're
                        // integers. Do the conversion so comparisons will work.
                        'ids' => [
                            '$map' => [
                                'input' => '$deckList.id',
                                'as' => 'cardId',
                                'in' => [
                                    '$toInt' => '$$cardId'
                                ]
                            ]
                        ]
                    ],
                    'in' => [
                        '$map' => [
                            'input' => [
                                // Generate an entry for the front and for the back, but filter out invalid ones.
                                '$filter' => [
                                    'input' => [
                                        [
                                            'end' => 'front',
                                            'bound' => '$$bounds.frontBound',
                                            'numCards' => [
                                                '$sum' => [
                                                    '$slice' => [
                                                        '$$quantities', '$$bounds.frontBound'
                                                    ]
                                                ]
                                            ],
                                            'idsToCheck' => [
                                                '$slice' => [
                                                    '$$ids', '$$bounds.frontBound'
                                                ]
                                            ],
                                            // There should be vanishingly few games with the same card in multiple
                                            // places in the decklist, but check anyway just in case.
                                            'ambiguousIds' => [
                                                '$setIntersection' => [
                                                    [
                                                        '$slice' => [
                                                            '$$ids', '$$bounds.frontBound'
                                                        ]
                                                    ], [
                                                        '$slice' => [
                                                            '$$ids', '$$bounds.frontBound', 60
                                                        ]
                                                    ]
                                                ]
                                            ]
                                        ], [
                                            'end' => 'back',
                                            'bound' => '$$bounds.backBound',
                                            'numCards' => [
                                                '$sum' => [
                                                    '$slice' => [
                                                        '$$quantities', '$$bounds.backBound', 60
                                                    ]
                                                ]
                                            ],
                                            'idsToCheck' => [
                                                '$slice' => [
                                                    '$$ids', '$$bounds.backBound', 60
                                                ]
                                            ],
                                            // There should be vanishingly few games with the same card in multiple
                                            // places in the decklist, but check anyway just in case.
                                            'ambiguousIds' => [
                                                '$setIntersection' => [
                                                    [
                                                        '$slice' => [
                                                            '$$ids', '$$bounds.backBound'
                                                        ]
                                                    ], [
                                                        '$slice' => [
                                                            '$$ids', '$$bounds.backBound', 60
                                                        ]
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ],
                                    'as' => 'info',
                                    'cond' => [
                                        // bound = -1 is used to indicate there's no acceptable number of cards that
                                        // will work. If ambiguousIds is not empty, then there's a card that has copies
                                        // both inside and outside the relevant section of the decklist. In either case,
                                        // drop the entry.
                                        '$and' => [
                                            [
                                                '$ne' => [
                                                    '$$info.bound', -1
                                                ]
                                            ], [
                                                '$eq' => [[ '$size' => '$$info.ambiguousIds' ], 0]
                                            ]
                                        ]
                                    ]
                                ]
                            ],
                            'as' => 'info',
                            'in' => [
                                'end' => '$$info.end',
                                'numCards' => '$$info.numCards',
                                // handsDrawn in the input is a 2 dimensional array, with each 1 dimensional array being
                                // a hand that was kept or mulliganed, and each value in a hand being a card id. Reduce
                                // each hand to a count of how many cards in it came from the region of the decklist
                                // being considered.
                                'handCounts' => [
                                    '$map' => [
                                        'input' => '$handsDrawn',
                                        'as' => 'hand',
                                        'in' => [
                                            '$size' => [
                                                '$filter' => [
                                                    'input' => '$$hand',
                                                    'as' => 'card',
                                                    'cond' => [
                                                        '$in' => [
                                                            '$$card', '$$info.idsToCheck'
                                                        ]
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ], [
        '$unwind' => [
            'path' => '$stats'
        ]
    ], [
        '$group' => [
            '_id' => [
                'end' => '$stats.end',
                'numCards' => '$stats.numCards',
                'update' => '$update'
            ],
            'date' => [
                '$max' => '$date'
            ],
            // stats.handCounts from the previous projection is a 1 dimensional array, with each value being the number
            // of cards from one end of the decklist that were in a hand drawn in the game, in the same order as the
            // hands were drawn.
            'handCounts' => [
                '$push' => '$stats.handCounts'
            ]
        ]
    ], [
        '$project' => [
            'date' => 1,
            // distribution will be a 2 dimensional array, structured as:
            //      [
            //          // First index is number of mulligans.
            //          [
            //              // Second index is number of cards from the front/back of the decklist in the drawn hand.
            //              <Number of games that match the indices>
            //          ]
            //      ]
            'distribution' => [
                '$map' => [
                    // Size of the hand at each value of the outer index
                    'input' => [
                        '$range' => [
                            7, 0, -1
                        ]
                    ],
                    'as' => 'handSize',
                    'in' => [
                        '$map' => [
                            // Inner index, number of cards from the relevant end of the decklist in the hand. Cover
                            // every value from 0 lands to the entire hand.
                            'input' => [
                                '$range' => [
                                    0, [
                                        '$add' => [
                                            '$$handSize', 1
                                        ]
                                    ]
                                ]
                            ],
                            'as' => 'count',
                            'in' => [
                                // Count number of games that match the indices.
                                '$size' => [
                                    '$filter' => [
                                        'input' => '$handCounts',
                                        'as' => 'hand',
                                        'cond' => [
                                            '$eq' => [
                                                [
                                                    '$arrayElemAt' => [
                                                        '$$hand', [
                                                            '$subtract' => [
                                                                7, '$$handSize'
                                                            ]
                                                        ]
                                                    ]
                                                ], '$$count'
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ], [
        // Need to merge with previously aggregated data.
        '$lookup' => [
            'from' => 'position_stats',
            'localField' => '_id',
            'foreignField' => '_id',
            'as' => 'stats'
        ]
    ], [
        '$project' => [
            'date' => 1,
            'distribution' => [
                '$cond' => [
                    // If there is no previously aggregated data for this group, then save some time and skip the merge.
                    [
                        '$eq' => [
                            [
                                '$size' => '$stats'
                            ], 0
                        ]
                    ], '$distribution', [
                        '$map' => [
                            // Pair up same-mulligans 1 dimensional inner arrays.
                            'input' => [
                                '$zip' => [
                                    'inputs' => [
                                        '$distribution', [
                                            // There can only ever be one matching doc to merge with, so extract it.
                                            '$arrayElemAt' => [
                                                '$stats.distribution', 0
                                            ]
                                        ]
                                    ]
                                ]
                            ],
                            'as' => 'handSizeStats',
                            'in' => [
                                '$map' => [
                                    // Pair up the actual values that have both indices matching, one value from new
                                    // data, one from old.
                                    'input' => [
                                        '$zip' => [
                                            'inputs' => [
                                                [
                                                    '$arrayElemAt' => [
                                                        '$$handSizeStats', 0
                                                    ]
                                                ], [
                                                    '$arrayElemAt' => [
                                                        '$$handSizeStats', 1
                                                    ]
                                                ]
                                            ]
                                        ]
                                    ],
                                    'as' => 'countStats',
                                    // Finally, add the paired-up values.
                                    'in' => [
                                        '$sum' => '$$countStats'
                                    ]
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ]
];

$updatesBatch = [];
$ready = 0;
$cursor = $mongo_matches->aggregate($position_pipeline, [ 'allowDiskUse' => TRUE ]);
foreach ($cursor as $document) {
    $updatesBatch[] = [ 'replaceOne' => [[ '_id' => $document['_id'] ], $document, [ 'upsert' => TRUE ]] ];
    $ready++;
    if ($ready === 100) {
        $mongo_positions->bulkWrite($updatesBatch, [ 'ordered' => FALSE ]);
        $updatesBatch = [];
        $ready = 0;
    }
}
if ($ready !== 0) {
    $mongo_positions->bulkWrite($updatesBatch, [ 'ordered' => FALSE ]);
}
