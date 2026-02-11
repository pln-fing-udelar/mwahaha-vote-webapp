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
let turnstileToken = null;

const PROLIFIC_MAX_BATTLES = 100;

const prolificId = (new URLSearchParams(window.location.search)).get("PROLIFIC_PID");
const isAProlificSession = Boolean(prolificId);
let prolificVoteCount = 0;

let $prolificConsent;
let $prolificVoteCount;

const translations = {
  en: {
    voteLeft: "Left is funnier. Thanks!",
    voteRight: "Right is funnier. Thanks!",
    voteTie: "It's a tie. Thanks!",
    voteSkip: "Battle skipped. Thanks!",
    error: "Sorry, an error occurred! Please try again later.",
    progress: "Progress",
    consent: {
      title: "Information and Consent Form",
      intro: "Please read the following information carefully. We recommend that you save a screenshot of this page.",
      researchers: {
        title: "Who are the researchers?",
        text: "The researchers behind this work are Santiago Castro, Luis Chiruzzo, Naihao Deng, Julie-Anne Meaney, Santiago Góngora, Ignacio Sastre, Victoria Amoroso, Guillermo Rey, Salar Rahili, Guillermo Moncecchi, Juan José Prada, Aiala Rosá, and Rada Mihalcea."
      },
      purpose: {
        title: "What is the purpose of this study?",
        text: "The purpose of this annotation process is to determine which texts are considered funnier by people."
      },
      why: {
        title: "Why have I been asked to be part of this study?",
        text: "This research is looking for annotators who are native speakers of the language from the texts."
      },
      required: {
        title: "Am I required to participate?",
        text: 'No. Participation in this study is entirely voluntary. You may withdraw from the study at any time, without giving any explanation. Your rights will not be affected. If you wish to withdraw from the study, please contact us at <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a>. We will not use your data in any publications or presentations if you have withdrawn from the study. However, we will keep a copy of the original consent form and your request to withdraw.'
      },
      risk: {
        title: "Is there any risk associated with being part of this study?",
        text: "There are no significant risks associated with participation. However, some texts may be offensive. You are also welcome to flag any text as offensive using the checkboxes in the annotation form. If at any time you wish to withdraw, please do not hesitate to do so."
      },
      benefit: {
        title: "Is there any benefit associated with being part of this study?",
        text: "There are no significant benefits associated with being part of the study, but you might find some texts funny."
      },
      contact: {
        title: "How can I contact you?",
        text: 'If you have further questions about the study or wish to make a complaint about it, please contact <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a>. When contacting us, please indicate the title of the study and the nature of your complaint.'
      },
      checkbox1: "I confirm that I have read and understood the information indicated above, that I have had the opportunity to ask questions, and that the questions I have had have been answered satisfactorily.",
      checkbox2: "I confirm that I am not a participant of this competition (i.e., I have not submitted a system to the 2025-2026 MWAHAHA competition on Humor Generation). Participants can't participate in the annotation process.",
      checkbox3: "I understand that my participation is voluntary, and that I can withdraw at any time without giving a reason. Withdrawing will not affect my rights.",
      checkbox4: "I give my consent for my anonymized data to be used in academic publications and presentations.",
      checkbox5: "I understand that my anonymized data may be stored for at least 2 years.",
      checkbox6: "I allow my data to be used ethically in future research.",
      checkbox7: "I agree to be part of this study.",
      continue: "Continue"
    },
    finished: {
      title: "You finished annotating!",
      prompt: "Do you have any suggestion, comment, or complaint?",
      commentsLabel: "Comments",
      finish: "Finish"
    }
  }, es: {
    voteLeft: "El de la izquierda es más gracioso. ¡Gracias!",
    voteRight: "El de la derecha es más gracioso. ¡Gracias!",
    voteTie: "Es un empate. ¡Gracias!",
    voteSkip: "Batalla omitida. ¡Gracias!",
    error: "¡Lo sentimos, ocurrió un error! Por favor, inténtalo de nuevo más tarde.",
    progress: "Progreso",
    consent: {
      title: "Formulario de Información y Consentimiento",
      intro: "Por favor, lea la siguiente información cuidadosamente. Le recomendamos guardar una captura de pantalla de esta página.",
      researchers: {
        title: "¿Quiénes son los investigadores?",
        text: "Los investigadores detrás de este trabajo son Santiago Castro, Luis Chiruzzo, Naihao Deng, Julie-Anne Meaney, Santiago Góngora, Ignacio Sastre, Victoria Amoroso, Guillermo Rey, Salar Rahili, Guillermo Moncecchi, Juan José Prada, Aiala Rosá y Rada Mihalcea."
      },
      purpose: {
        title: "¿Cuál es el propósito de este estudio?",
        text: "El propósito de este proceso de anotación es determinar qué textos se consideran más graciosos por las personas."
      },
      why: {
        title: "¿Por qué me han pedido que participe en este estudio?",
        text: "Esta investigación busca anotadores que sean hablantes nativos del idioma de los textos."
      },
      required: {
        title: "¿Estoy obligado a participar?",
        text: 'No. La participación en este estudio es completamente voluntaria. Usted puede retirarse del estudio en cualquier momento, sin dar ninguna explicación. Sus derechos no se verán afectados. Si desea retirarse del estudio, por favor contáctenos en <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a>. No usaremos sus datos en ninguna publicación o presentación si se ha retirado del estudio. Sin embargo, conservaremos una copia del formulario de consentimiento original y su solicitud de retiro.'
      },
      risk: {
        title: "¿Hay algún riesgo asociado con formar parte de este estudio?",
        text: "No hay riesgos significativos asociados con la participación. Sin embargo, algunos textos pueden ser ofensivos. También puede marcar cualquier texto como ofensivo usando las casillas de verificación en el formulario de anotación. Si en algún momento desea retirarse, no dude en hacerlo."
      },
      benefit: {
        title: "¿Hay algún beneficio asociado con formar parte de este estudio?",
        text: "No hay beneficios significativos asociados con formar parte del estudio, pero puede encontrar algunos textos graciosos."
      },
      contact: {
        title: "¿Cómo puedo contactarlos?",
        text: 'Si tiene más preguntas sobre el estudio o desea presentar una queja, por favor contacte a <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a>. Al contactarnos, por favor indique el título del estudio y la naturaleza de su queja.'
      },
      checkbox1: "Confirmo que he leído y comprendido la información indicada anteriormente, que he tenido la oportunidad de hacer preguntas, y que las preguntas que he tenido han sido respondidas satisfactoriamente.",
      checkbox2: "Confirmo que no soy participante de esta competencia (es decir, no he enviado un sistema a la competencia MWAHAHA 2025-2026 sobre Generación de Humor). Los participantes no pueden participar en el proceso de anotación.",
      checkbox3: "Entiendo que mi participación es voluntaria, y que puedo retirarme en cualquier momento sin dar una razón. Retirarme no afectará mis derechos.",
      checkbox4: "Doy mi consentimiento para que mis datos anonimizados sean utilizados en publicaciones y presentaciones académicas.",
      checkbox5: "Entiendo que mis datos anonimizados pueden ser almacenados durante al menos 2 años.",
      checkbox6: "Permito que mis datos sean utilizados éticamente en futuras investigaciones.",
      checkbox7: "Acepto formar parte de este estudio.",
      continue: "Continuar"
    },
    finished: {
      title: "¡Terminaste de anotar!",
      prompt: "¿Tienes alguna sugerencia, comentario o queja?",
      commentsLabel: "Comentarios",
      finish: "Finalizar"
    }
  }, zh: {
    voteLeft: "左边更有趣。",
    voteRight: "右边更有趣。",
    voteTie: "平局。",
    voteSkip: "跳过此轮。",
    error: "抱歉，发生错误, 请稍后再试。",
    progress: "进度",
    consent: {
      title: "信息说明与知情同意书",
      intro: "请仔细阅读以下信息。建议您保存或截图本页面以备参考。",
      researchers: {
        title: "研究人员",
        text: "本研究的研究人员包括：Santiago Castro、Luis Chiruzzo、Naihao Deng、Julie-Anne Meaney、Santiago Góngora、Ignacio Sastre、Victoria Amoroso、Guillermo Rey、Salar Rahili、Guillermo Moncecchi、Juan José Prada、Aiala Rosá 和 Rada Mihalcea。"
      },
      purpose: {
        title: "本研究的目的是什么？", text: "本次标注旨在确定人们认为哪些文本更具幽默性。"
      },
      why: {
        title: "为什么邀请我参加本研究？", text: "本研究正在招募母语者参与文本标注。"
      },
      required: {
        title: "我必须参加吗？",
        text: '不必须。参与本研究完全出于自愿。您可以在任何时候退出研究，无需说明理由，且不会对您的权利产生任何影响。如您希望退出，请通过 <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a> 与我们联系。如果您退出研究，我们将不会在任何出版物或展示中使用您的数据。但我们将保留原始同意书及您的退出请求记录。'
      },
      risk: {
        title: "参与本研究是否存在任何风险？",
        text: "参与本研究不存在重大风险。但部分文本可能包含冒犯性内容。您可以通过标注表单中的复选框将相关文本标记为冒犯性内容。如感到不适，您可随时退出研究。"
      },
      benefit: {
        title: "参与本研究是否有任何益处？", text: "参与本研究不会带来直接的个人收益，但部分文本内容可能具有一定的趣味性。"
      },
      contact: {
        title: "如何联系我们？",
        text: '如您对本研究有任何疑问或希望提出投诉，请联系 <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a>。联系时请注明研究标题及问题或投诉的具体内容。'
      },
      checkbox1: "我确认已阅读并理解上述信息，并有机会提出问题（如有疑问），且已获得满意的答复。",
      checkbox2: "我确认我不是本次竞赛的参与者（即未参与 2025–2026 年 MWAHAHA 幽默生成竞赛）。参赛者不得参与本次标注。",
      checkbox3: "我理解我的参与是自愿的，并且可以在任何时候退出而无需说明理由，且不会影响我的权利。",
      checkbox4: "我同意将我的匿名数据用于学术出版和展示。",
      checkbox5: "我理解我的匿名数据可能至少保存2年。",
      checkbox6: "我同意在符合伦理规范的前提下，将我的数据用于未来的相关研究。",
      checkbox7: "我同意参加本研究。",
      continue: "继续"
    },
    finished: {
      title: "您已完成标注！", prompt: "您有任何建议、评论或投诉吗？", commentsLabel: "评论", finish: "完成"
    }
  }
};

$(document).ready(main);

// noinspection JSUnusedGlobalSymbols
function onTurnstileSuccess(token) {
  turnstileToken = token;
}

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
  if (!turnstileToken) {
    $.mdtoast("Please complete the verification challenge (CAPTCHA)", {duration: 3000});
    return;
  }

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
    turnstile_token: turnstileToken,
  }, battle => {
    battles[oldIndex] = battle;

    if (typeof turnstile !== 'undefined') {
      turnstile.reset();
    }
    turnstileToken = null;
  }, "json").fail(() => {
    const lang = getLanguageFromTask();
    $.mdtoast(translations[lang].error, {duration: 3000});

    if (typeof turnstile !== 'undefined') {
      turnstile.reset();
    }
    turnstileToken = null;
  });

  showBattle();

  $.mdtoast(toastText(voteOption), {duration: 3000});

  $isOffensiveLeft.prop("checked", false);
  $isOffensiveRight.prop("checked", false);

  if (isAProlificSession && voteOption !== "n") {
    prolificVoteCount++;
    updateProlificVoteCount();
  }
}

function getLanguageFromTask() {
  return task.startsWith("a-") ? task.split("-")[1] : "en";
}

function translateModal(lang) {
  $("[data-i18n]").each(function () {
    const key = $(this).data("i18n");
    const keys = key.split(".");
    let value = translations[lang];
    for (let i = 0; i < keys.length; i++) {
      value = value[keys[i]];
    }
    $(this).text(value);
  });

  $("[data-i18n-html]").each(function () {
    const key = $(this).data("i18n-html");
    const keys = key.split(".");
    let value = translations[lang];
    for (let i = 0; i < keys.length; i++) {
      value = value[keys[i]];
    }
    $(this).html(value);
  });
}

function toastText(voteOption) {
  const lang = getLanguageFromTask();
  const t = translations[lang];

  switch (voteOption) {
    case "a":
      return t.voteLeft;
    case "b":
      return t.voteRight;
    case "t":
      return t.voteTie;
    default:
      return t.voteSkip;
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
    const lang = getLanguageFromTask();
    const t = translations[lang];
    $prolificVoteCount.text(`${t.progress}: ${prolificVoteCount}/${PROLIFIC_MAX_BATTLES}`);
  }
}

function setupProlificSessionIfNeeded() {
  if (isAProlificSession) {
    const lang = getLanguageFromTask();

    translateModal(lang);

    $prolificConsent.find("form").submit(e => {
      localStorage.setItem(`consent-prolific-id-${prolificId}`, "done");
      $prolificConsent.modal("hide");

      $("#helpModal").modal("show");

      e.preventDefault();
      e.stopPropagation();
    });

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
