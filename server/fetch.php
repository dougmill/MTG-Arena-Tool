<?php

require_once(join(DIRECTORY_SEPARATOR,[__DIR__,'..','api.php']));
$api = Api::getInstance();

parse_str(file_get_contents('php://input'), $query);
if (array_key_exists('deckSize', $query) && array_key_exists('landsInDeck', $query) && array_key_exists('bestOf', $query)) {
    $mongo_lands = (new MongoDB\Client)->tracker->land_stats;
    // This one's simple, just return what's in the db. Using aggregate only to rename the id field.
    $pipeline = [
        [
            '$match' => [
                '_id.deckSize' => (int)$query['deckSize'],
                '_id.landsInDeck' => (int)$query['landsInDeck'],
                '_id.bestOf' => (int)$query['bestOf']
            ]
        ], [
            '$project' => [
                '_id' => 0,
                'date' => 1,
                'group' => '$_id',
                'distribution' => 1
            ]
        ]
    ];

    $cursor = $mongo_lands->aggregate($pipeline, [ 'allowDiskUse' => TRUE ]);
    echo json_encode(iterator_to_array($cursor, false));
} else {
    // This query is more complicated. It will return combined totals for each configuration of the library, ignoring
    // deck size, lands in deck, and Bo1 vs Bo3.
    $mongo_lands = (new MongoDB\Client)->tracker->land_stats;
    $lands_pipeline = [
        [
            '$group' => [
                '_id' => [
                    'librarySize' => '$_id.librarySize',
                    'landsInLibrary' => '$_id.landsInLibrary',
                    'shuffling' => '$_id.shuffling'
                ],
                'date' => [
                    '$max' => '$date'
                ],
                // Coming from each record, distribution will be a 3 dimensional array, structured as:
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
                // distributions is then a 4 dimensional array.
                'distributions' => [
                    '$push' => '$distribution'
                ]
            ]
        ], [
            '$project' => [
                '_id' => 0,
                'date' => 1,
                'group' => '$_id',
                // This calculation is very similar to the one for merging new and old statistics in the main
                // aggregation code.
                'distribution' => [
                    '$reduce' => [
                        // Go through the distributions array, adding up totals as we go.
                        'input' => '$distributions',
                        'initialValue' => [],
                        'in' => [
                            '$map' => [
                                // Pair up same-position 2 dimensional middle arrays.
                                'input' => [
                                    '$zip' => [
                                        'inputs' => [
                                            '$$this', '$$value'
                                        ],
                                        'useLongestLength' => TRUE,
                                        'defaults' => [
                                            [], []
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
                                                ],
                                                'useLongestLength' => TRUE,
                                                'defaults' => [
                                                    [], []
                                                ]
                                            ]
                                        ],
                                        'as' => 'countStats',
                                        'in' => [
                                            '$map' => [
                                                // Pair up the actual values that have all three indices matching, one
                                                // value from accumulated data, one from new entry.
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
                                                        ],
                                                        'useLongestLength' => TRUE,
                                                        'defaults' => [
                                                            [], []
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

    $cursor = $mongo_lands->aggregate($lands_pipeline, [ 'allowDiskUse' => TRUE ]);
    echo '{"lands":';
    echo json_encode(iterator_to_array($cursor, false));

    // Hand, card, and position stats take up little enough space that it's reasonable to return it all.
    $mongo_hands = (new MongoDB\Client)->tracker->hand_stats;
    $pipeline = [[
        '$project' => [
            '_id' => 0,
            'date' => 1,
            'group' => '$_id',
            'distribution' => 1
        ]
    ]];
    $cursor = $mongo_hands->aggregate($pipeline, [ 'allowDiskUse' => TRUE ]);
    echo ',"hands":';
    echo json_encode(iterator_to_array($cursor, false));

    $mongo_cards = (new MongoDB\Client)->tracker->card_stats;
    $cursor = $mongo_cards->aggregate($pipeline, [ 'allowDiskUse' => TRUE ]);
    echo ',"cards":';
    echo json_encode(iterator_to_array($cursor, false));

    $mongo_positions = (new MongoDB\Client)->tracker->position_stats;
    $cursor = $mongo_positions->aggregate($pipeline, [ 'allowDiskUse' => TRUE ]);
    echo ',"positions":';
    echo json_encode(iterator_to_array($cursor, false));
    echo '}';
}