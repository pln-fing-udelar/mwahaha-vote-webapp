let $star;
let $homeContent;
let $prompt;
let $outputA;
let $outputB;
let $votesAndToolbox;
// let $toolbox;
let $voteLeft;
let $voteRight;
let $legendVote;
let $skip;
let $isOffensiveLeft;
let $isOffensiveRight;
let emoji;

let battles = [];
let index = 0;

// function getParameterByName(name, url = window.location.href) {
//     name = name.replace(/[\[\]]/g, "\\$&");
//     const regex = new RegExp(`[?&]${name}(=([^&#]*)|&|#|$)`);
//     const results = regex.exec(url);
//     if (!results) {
//         return null;
//     }
//     if (!results[2]) {
//         return "";
//     }
//     return decodeURIComponent(results[2].replace(/\+/g, " "));
// }

$(document).ready(main);

function main() {
    setupSentry();
    setupElements();
    setupPlaceload();
    setupEmojiConverter();
    getRandomBattles();
    setUiListeners();
    moveToolboxIfOutside();
}

function setupSentry() {
    // The following key is public.
    // Raven.config("https://3afb3f9917f44b2a87e6fbb070a8977b@sentry.io/298102", {
    //     ignoreUrls: ["localhost", "127.0.0.1"]
    // }).install();
}

function setupElements() {
    $star = $("*");
    $homeContent = $("#home-content");
    $prompt = $("#prompt-text");
    $outputA = $("#output-a-text");
    $outputB = $("#output-b-text");
    $voteLeft = $("#vote-left");
    $voteRight = $("#vote-right");
    $votesAndToolbox = $("#votes,#toolbox");
    // $toolbox = $("#toolbox");
    $legendVote = $(".legend-vote");
    $skip = $("#skip");
    $isOffensiveLeft = $("#is-offensive-left");
    $isOffensiveRight = $("#is-offensive-right");
}

function showBattle() {
    if (battles.length === 0) {
        console.error("There are no battles to display.");
    } else {
        $prompt.fadeOut(100, () => {
            // TODO: add the image.
            $prompt.html(emoji.replace_unified(battles[index].prompt.replace(/\n/mg, "<br/>"))).text();
            $prompt.fadeIn(100);
        });
        $outputA.fadeOut(100, () => {
            $outputA.html(emoji.replace_unified(battles[index].output_a.replace(/\n/mg, "<br/>"))).text();
            $outputA.fadeIn(100);
        });
        $outputB.fadeOut(100, () => {
            $outputB.html(emoji.replace_unified(battles[index].output_b.replace(/\n/mg, "<br/>"))).text();
            $outputB.fadeIn(100);
        });
    }
}

function setupPlaceload() {
    Placeload
        .$("#output-a-text")
        .config({speed: "1s"})
        .line(element => element.width(100).height(15))
        .config({spaceBetween: "7px"})
        .line(element => element.width(100).height(15))
        .config({spaceBetween: "7px"})
        .line(element => element.width(40).height(15)).fold(() => {
    }, () => {
    });
    Placeload
        .$("#output-b-text")
        .config({speed: "1s"})
        .line(element => element.width(100).height(15))
        .config({spaceBetween: "7px"})
        .line(element => element.width(100).height(15))
        .config({spaceBetween: "7px"})
        .line(element => element.width(40).height(15)).fold(() => {
    }, () => {
    });
}

function setupEmojiConverter() {
    // noinspection JSUnresolvedFunction
    emoji = new EmojiConvertor();
    emoji.img_set = "twitter";
    emoji.img_sets.twitter.path = "https://raw.githubusercontent.com/iamcal/emoji-data/" + "a97b2d2efa64535d6300660eb2cd15ecb584e79e/img-twitter-64/";
}

function getRandomBattles() {
    $.getJSON("battles", data => {
        battles = data;
        showBattle();
    });
}

function setUiListeners() {
    $voteLeft.click(() => vote("a"));
    $voteRight.click(() => vote("b"));
    $skip.click(() => vote("n"));
    $("#answers button").mouseup(e => $(e.currentTarget).blur());
}

function vote(voteOption) {
    const oldIndex = index;
    index = (index + 1) % battles.length;

    const otherIndex = (index + 1) % battles.length;

    $.post("vote", {
        task: "a-en",
        prompt_id: battles[oldIndex].prompt_id,
        system_id_a: battles[oldIndex].system_id_a,
        system_id_b: battles[oldIndex].system_id_b,
        vote: voteOption,
        ignore_output_ids: [
            battles[index].prompt_id + "-" + battles[index].system_id_a,
            battles[index].prompt_id + "-" + battles[index].system_id_b,
            battles[otherIndex].prompt_id + "-" + battles[otherIndex].system_id_a,
            battles[otherIndex].prompt_id + "-" + battles[otherIndex].system_id_b,
        ],
        is_offensive_a: $isOffensiveLeft.prop("checked"),
        is_offensive_b: $isOffensiveRight.prop("checked"),
    }, battle => battles[oldIndex] = battle, "json");

    showBattle();

    $.mdtoast(toastText(voteOption), {duration: 3000});

    $votesAndToolbox.fadeOut();

    $isOffensiveLeft.prop("checked", false);
    $isOffensiveRight.prop("checked", false);
}

function toastText(voteOption) {
    if (voteOption === "a") {
        return "Left is better. Thanks!";
    } else if (voteOption === "b") {
        return "Right is better. Thanks!";
    } else {
        return "Battle skipped. Thanks!";
    }
}

function moveToolboxIfOutside() {
    // const x = $toolbox[0].getBoundingClientRect().x;
    // if (x < 0) {
    //     const translation = -x + 10;
    //     addPxToLeft($toolbox, translation);
    //     addPxToLeft($vote1, translation);
    //     addPxToLeft($vote2, translation);
    //     addPxToLeft($vote3, translation);
    //     addPxToLeft($vote4, translation);
    //     addPxToLeft($vote5, translation);
    // }
}

// function addPxToLeft(element, translation) {
//     element.css("left", `${(parseInt(element.css("left")) + translation)}px`);
// }
