let $star;
let $homeContent;
let $task;
let $prompt;
let $outputA;
let $outputB;
let $answerButtons;
let $isOffensiveLeft;
let $voteLeft;
let $tie;
let $skip;
let $voteRight;
let $isOffensiveRight;
let emoji;

let task = "a-en";
let battles = [];
let index = 0;

const PROLIFIC_MAX_BATTLES = 100;

const prolificId = (new URLSearchParams(window.location.search)).get("PROLIFIC_PID");
const isAProlificSession = Boolean(prolificId);
let prolificVoteCount = 0;

let $prolificConsent;
let $prolificVoteCount;

$(document).ready(main);

function main() {
  setupSentry();
  setupElements();
  setupPlaceload();
  setupEmojiConverter();
  getTask();
  getRandomBattles();
  setUiListeners();
  setupProlificSessionIfNeeded();
}

function setupSentry() {
  // The following key is public.
  window.sentryOnLoad = () => Sentry.init({
    dsn: "https://85c805830c8feb48a02488382cecd1a5@o134888.ingest.us.sentry.io/4510338807627776", sendDefaultPii: true, // Alternatively, use `process.env.npm_package_version` for a dynamic release version
    // if your build tool supports it.
    release: "mwahahavote@0.1.0", denyUrls: [/https?:\/\/localhost/, /https?:\/\/127\.0\.0\.1/, /https?:\/\/\[::1?]/],
  });
}

function setupElements() {
  $star = $("*");
  $homeContent = $("#home-content");
  $task = $("#task");
  $prompt = $("#prompt-text");
  $outputA = $("#output-a-text");
  $outputB = $("#output-b-text");
  $answerButtons = $("#answers button");
  $isOffensiveLeft = $("#is-offensive-left");
  $voteLeft = $("#vote-left");
  $tie = $("#tie");
  $skip = $("#skip");
  $voteRight = $("#vote-right");
  $isOffensiveRight = $("#is-offensive-right");
}

function showBattle() {
  if (battles.length === 0) {
    $("#prompt").fadeOut(0);
    $("#output-a").fadeOut(0);
    $("#output-b").fadeOut(0);
    $("#outputs").append("<p class=\"col-xs-12\" style=\"padding: 10px\">There are no battles to display for this task.</p>");
    $("#answers").fadeOut(0);
  } else {
    $prompt.fadeOut(100, () => {
      const prompt_image_url = battles[index].prompt_image_url;
      let imageHtml = "";
      if (prompt_image_url) {
        imageHtml = '<img id="prompt-image" src="' + prompt_image_url + '" alt="Prompt image"><br/>';
      }
      $prompt.html(imageHtml + emoji.replace_unified(battles[index].prompt || "").replace(/\n/mg, "<br/>")).text();
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
      .$("#prompt-text")
      .config({speed: "1s"})
      .line(element => element.width(100).height(15)).fold(() => {
  }, () => {
  });
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

function getTask() {
  const urlParams = new URLSearchParams(window.location.search);

  task = urlParams.get("task");

  if (task === null) {
    if (navigator.language.startsWith("es")) {
      task = "a-es";
    } else if (navigator.language.startsWith("zh")) {
      task = "a-zh";
    } else {
      task = "a-en";
    }
  }

  urlParams.set("task", task);

  $task.val(task);

  // We set the query param in the URL without refreshing the page:
  const newUrl = window.location.protocol + "//" + window.location.host + window.location.pathname + "?" + urlParams.toString();
  window.history.replaceState({}, "", newUrl);
}

function getRandomBattles() {
  $.getJSON("battles", {task: task}, data => {
    battles = data;
    showBattle();
  });
}

function setUiListeners() {
  $task.change(() => changeTask());

  $answerButtons.mouseup(e => {
    $(e.currentTarget).blur();
    $(e.currentTarget).addClass("no-hover");
  });
  $answerButtons.on("mousemove mousedown", e => $(e.currentTarget).removeClass("no-hover"));

  $voteLeft.click(() => vote("a"));
  $tie.click(() => vote("t"));
  $skip.click(() => vote("n"));
  $voteRight.click(() => vote("b"));

  $prolificConsent = $("#prolific-consent")
  $prolificVoteCount = $("#prolific-vote-count");
}

function changeTask() {
  window.location.href = "?task=" + encodeURIComponent($task.val());
}

function vote(voteOption) {
  const oldIndex = index;
  index = (index + 1) % battles.length;

  const otherIndex = (index + 1) % battles.length;

  $.post("vote", {
    prompt_id: battles[oldIndex].prompt_id,
    system_id_a: battles[oldIndex].system_id_a,
    system_id_b: battles[oldIndex].system_id_b,
    vote: voteOption,
    ignore_output_ids: [battles[index].prompt_id + "-" + battles[index].system_id_a, battles[index].prompt_id + "-" + battles[index].system_id_b, battles[otherIndex].prompt_id + "-" + battles[otherIndex].system_id_a, battles[otherIndex].prompt_id + "-" + battles[otherIndex].system_id_b,],
    is_offensive_a: $isOffensiveLeft.prop("checked"),
    is_offensive_b: $isOffensiveRight.prop("checked"),
  }, battle => battles[oldIndex] = battle, "json").fail(() => $.mdtoast("Sorry, an error occurred! Please try again later.", {duration: 3000}));

  showBattle();

  $.mdtoast(toastText(voteOption), {duration: 3000});

  $isOffensiveLeft.prop("checked", false);
  $isOffensiveRight.prop("checked", false);

  if (isAProlificSession && voteOption !== "n") {
    prolificVoteCount++;
    updateProlificVoteCount();
  }
}

function toastText(voteOption) {
  if (voteOption === "a") {
    return "Left is funnier. Thanks!";
  } else if (voteOption === "b") {
    return "Right is funnier. Thanks!";
  } else if (voteOption === "t") {
    return "It's a tie. Thanks!";
  } else {
    return "Battle skipped. Thanks!";
  }
}

function updateProlificVoteCount() {
  // The same person may annotate multiple times.
  // So we should only stop when they reach exactly the max (and not greater than the max).
  // And we should keep a modulo counter until they reach the max.
  if (prolificVoteCount === PROLIFIC_MAX_BATTLES) {
    $("#prolific-finished").modal("show");
  } else {
    prolificVoteCount %= PROLIFIC_MAX_BATTLES;
    $prolificVoteCount.text(`Progress: ${prolificVoteCount}/${PROLIFIC_MAX_BATTLES}`);
  }
}

function setupProlificSessionIfNeeded() {
  if (isAProlificSession) {
    $("#prolific-consent form").submit(e => {
      localStorage.setItem(`consent-prolific-id-${prolificId}`, "done");
      $prolificConsent.modal("hide");

      $("#helpModal").modal("show");

      e.preventDefault();
      e.stopPropagation();
    });

    // $skip.parent().css("display", "none");
    // $("#skip-instructions").css("display", "none");
    $task.prop("disabled", "disabled");

    if (localStorage.getItem(`consent-prolific-id-${prolificId}`) !== "done") {
      $("#prolific-id").val(prolificId);
      $prolificConsent.modal("show");
      $("#prolific-consent-continue").click(() => $.post("prolific-consent"));
    }

    $.getJSON("session-vote-count", count => {
      prolificVoteCount = count;
      updateProlificVoteCount();
      $prolificVoteCount.css("display", "block");
    });
  }
}
