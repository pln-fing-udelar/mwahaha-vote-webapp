let taskSelect;
let promptText;
let outputA;
let outputAText;
let outputB;
let outputBText;
let answerButtons;
let isOffensiveLeft;
let voteLeft;
let tie;
let skip;
let voteRight;
let isOffensiveRight;
let emoji;

let task = "t3";

let battles = [];
let index = 0;
let nextReplaceIndex = 0;

let turnstileToken = null;

const PROLIFIC_MAX_BATTLES = 100;

const prolificId = (new URLSearchParams(window.location.search)).get("PROLIFIC_PID");
const isAProlificSession = Boolean(prolificId);
let prolificVoteCount = 0;

let prolificConsent;
let prolificVoteCountEl;

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
        text: "The researchers behind this work are Santiago Castro, Luis Chiruzzo, Naihao Deng, Julie-Anne Meaney, Santiago Góngora, Ignacio Sastre, Victoria Amoroso, Guillermo Rey, Salar Rahili, Guillermo Moncecchi, Juan José Prada, Aiala Rosá, and Rada Mihalcea.",
      },
      purpose: {
        title: "What is the purpose of this study?",
        text: "The purpose of this annotation process is to determine which texts are considered funnier by people.",
      },
      why: {
        title: "Why have I been asked to be part of this study?",
        text: "This research is looking for annotators who are native speakers of the language from the texts.",
      },
      required: {
        title: "Am I required to participate?",
        text: 'No. Participation in this study is entirely voluntary. You may withdraw from the study at any time, without giving any explanation. Your rights will not be affected. If you wish to withdraw from the study, please contact us at <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a>. We will not use your data in any publications or presentations if you have withdrawn from the study. However, we will keep a copy of the original consent form and your request to withdraw.',
      },
      risk: {
        title: "Is there any risk associated with being part of this study?",
        text: "There are no significant risks associated with participation. However, some texts may be offensive. You are also welcome to flag any text as offensive using the checkboxes in the annotation form. If at any time you wish to withdraw, please do not hesitate to do so.",
      },
      benefit: {
        title: "Is there any benefit associated with being part of this study?",
        text: "There are no significant benefits associated with being part of the study, but you might find some texts funny.",
      },
      contact: {
        title: "How can I contact you?",
        text: 'If you have further questions about the study or wish to make a complaint about it, please contact <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a>. When contacting us, please indicate the title of the study and the nature of your complaint.',
      },
      checkbox1: "I confirm that I have read and understood the information indicated above, that I have had the opportunity to ask questions, and that the questions I have had have been answered satisfactorily.",
      checkbox2: "I confirm that I am not a participant of this competition (i.e., I have not submitted a system to the 2025-2026 MWAHAHA competition on Humor Generation). Participants can't participate in the annotation process.",
      checkbox3: "I understand that my participation is voluntary, and that I can withdraw at any time without giving a reason. Withdrawing will not affect my rights.",
      checkbox4: "I give my consent for my anonymized data to be used in academic publications and presentations.",
      checkbox5: "I understand that my anonymized data may be stored for at least 2 years.",
      checkbox6: "I allow my data to be used ethically in future research.",
      checkbox7: "I agree to be part of this study.",
      continue: "Continue",
    },
    finished: {
      title: "You finished annotating!",
      prompt: "Do you have any suggestion, comment, or complaint?",
      commentsLabel: "Comments",
      finish: "Finish",
    },
  },
  es: {
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
        text: "Los investigadores detrás de este trabajo son Santiago Castro, Luis Chiruzzo, Naihao Deng, Julie-Anne Meaney, Santiago Góngora, Ignacio Sastre, Victoria Amoroso, Guillermo Rey, Salar Rahili, Guillermo Moncecchi, Juan José Prada, Aiala Rosá y Rada Mihalcea.",
      },
      purpose: {
        title: "¿Cuál es el propósito de este estudio?",
        text: "El propósito de este proceso de anotación es determinar qué textos se consideran más graciosos por las personas.",
      },
      why: {
        title: "¿Por qué me han pedido que participe en este estudio?",
        text: "Esta investigación busca anotadores que sean hablantes nativos del idioma de los textos.",
      },
      required: {
        title: "¿Estoy obligado a participar?",
        text: 'No. La participación en este estudio es completamente voluntaria. Usted puede retirarse del estudio en cualquier momento, sin dar ninguna explicación. Sus derechos no se verán afectados. Si desea retirarse del estudio, por favor contáctenos en <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a>. No usaremos sus datos en ninguna publicación o presentación si se ha retirado del estudio. Sin embargo, conservaremos una copia del formulario de consentimiento original y su solicitud de retiro.',
      },
      risk: {
        title: "¿Hay algún riesgo asociado con formar parte de este estudio?",
        text: "No hay riesgos significativos asociados con la participación. Sin embargo, algunos textos pueden ser ofensivos. También puede marcar cualquier texto como ofensivo usando las casillas de verificación en el formulario de anotación. Si en algún momento desea retirarse, no dude en hacerlo.",
      },
      benefit: {
        title: "¿Hay algún beneficio asociado con formar parte de este estudio?",
        text: "No hay beneficios significativos asociados con formar parte del estudio, pero puede encontrar algunos textos graciosos.",
      },
      contact: {
        title: "¿Cómo puedo contactarlos?",
        text: 'Si tiene más preguntas sobre el estudio o desea presentar una queja, por favor contacte a <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a>. Al contactarnos, por favor indique el título del estudio y la naturaleza de su queja.',
      },
      checkbox1: "Confirmo que he leído y comprendido la información indicada anteriormente, que he tenido la oportunidad de hacer preguntas, y que las preguntas que he tenido han sido respondidas satisfactoriamente.",
      checkbox2: "Confirmo que no soy participante de esta competencia (es decir, no he enviado un sistema a la competencia MWAHAHA 2025-2026 sobre Generación de Humor). Los participantes no pueden participar en el proceso de anotación.",
      checkbox3: "Entiendo que mi participación es voluntaria, y que puedo retirarme en cualquier momento sin dar una razón. Retirarme no afectará mis derechos.",
      checkbox4: "Doy mi consentimiento para que mis datos anonimizados sean utilizados en publicaciones y presentaciones académicas.",
      checkbox5: "Entiendo que mis datos anonimizados pueden ser almacenados durante al menos 2 años.",
      checkbox6: "Permito que mis datos sean utilizados éticamente en futuras investigaciones.",
      checkbox7: "Acepto formar parte de este estudio.",
      continue: "Continuar",
    },
    finished: {
      title: "¡Terminaste de anotar!",
      prompt: "¿Tienes alguna sugerencia, comentario o queja?",
      commentsLabel: "Comentarios",
      finish: "Finalizar",
    },
  },
  zh: {
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
        text: "本研究的研究人员包括：Santiago Castro、Luis Chiruzzo、Naihao Deng、Julie-Anne Meaney、Santiago Góngora、Ignacio Sastre、Victoria Amoroso、Guillermo Rey、Salar Rahili、Guillermo Moncecchi、Juan José Prada、Aiala Rosá 和 Rada Mihalcea。",
      },
      purpose: {
        title: "本研究的目的是什么？",
        text: "本次标注旨在确定人们认为哪些文本更具幽默性。",
      },
      why: {
        title: "为什么邀请我参加本研究？",
        text: "本研究正在招募母语者参与文本标注。",
      },
      required: {
        title: "我必须参加吗？",
        text: '不必须。参与本研究完全出于自愿。您可以在任何时候退出研究，无需说明理由，且不会对您的权利产生任何影响。如您希望退出，请通过 <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a> 与我们联系。如果您退出研究，我们将不会在任何出版物或展示中使用您的数据。但我们将保留原始同意书及您的退出请求记录。',
      },
      risk: {
        title: "参与本研究是否存在任何风险？",
        text: "参与本研究不存在重大风险。但部分文本可能包含冒犯性内容。您可以通过标注表单中的复选框将相关文本标记为冒犯性内容。如感到不适，您可随时退出研究。",
      },
      benefit: {
        title: "参与本研究是否有任何益处？",
        text: "参与本研究不会带来直接的个人收益，但部分文本内容可能具有一定的趣味性。",
      },
      contact: {
        title: "如何联系我们？",
        text: '如您对本研究有任何疑问或希望提出投诉，请联系 <a href="mailto:sacastro@fing.edu.uy">sacastro@fing.edu.uy</a>。联系时请注明研究标题及问题或投诉的具体内容。',
      },
      checkbox1: "我确认已阅读并理解上述信息，并有机会提出问题（如有疑问），且已获得满意的答复。",
      checkbox2: "我确认我不是本次竞赛的参与者（即未参与 2025–2026 年 MWAHAHA 幽默生成竞赛）。参赛者不得参与本次标注。",
      checkbox3: "我理解我的参与是自愿的，并且可以在任何时候退出而无需说明理由，且不会影响我的权利。",
      checkbox4: "我同意将我的匿名数据用于学术出版和展示。",
      checkbox5: "我理解我的匿名数据可能至少保存2年。",
      checkbox6: "我同意在符合伦理规范的前提下，将我的数据用于未来的相关研究。",
      checkbox7: "我同意参加本研究。",
      continue: "继续",
    },
    finished: {
      title: "您已完成标注！",
      prompt: "您有任何建议、评论或投诉吗？",
      commentsLabel: "评论",
      finish: "完成",
    },
  },
};

// Vanilla JS Modal Management
function showModal(modalEl) {
  if (typeof modalEl === "string") {
    modalEl = document.querySelector(modalEl);
  }
  if (!modalEl || modalEl.classList.contains("in")) return;

  document.body.classList.add("modal-open");

  let backdrop = document.querySelector(".modal-backdrop");
  if (!backdrop) {
    backdrop = document.createElement("div");
    backdrop.className = "modal-backdrop fade";
    document.body.appendChild(backdrop);
    // Force reflow for CSS transition
    void backdrop.offsetHeight;
    backdrop.classList.add("in");
  }

  modalEl.style.display = "block";
  void modalEl.offsetHeight;
  modalEl.classList.add("in");
  modalEl.setAttribute("aria-hidden", "false");
}

function hideModal(modalEl) {
  if (typeof modalEl === "string") {
    modalEl = document.querySelector(modalEl);
  }
  if (!modalEl) return;

  modalEl.classList.remove("in");

  const backdrop = document.querySelector(".modal-backdrop");
  if (backdrop) {
    backdrop.classList.remove("in");
  }

  setTimeout(() => {
    modalEl.style.display = "none";
    modalEl.setAttribute("aria-hidden", "true");

    const openModals = document.querySelectorAll(".modal.in");
    if (openModals.length === 0) {
      document.body.classList.remove("modal-open");
      if (backdrop && backdrop.parentNode) {
        backdrop.parentNode.removeChild(backdrop);
      }
    }
  }, 150);
}

function setupModalDelegation() {
  document.addEventListener("click", e => {
    const toggleBtn = e.target.closest("[data-toggle='modal']");
    if (toggleBtn) {
      e.preventDefault();
      const targetSelector = toggleBtn.getAttribute("data-target") || toggleBtn.getAttribute("href");
      if (targetSelector && targetSelector !== "#") {
        showModal(targetSelector);
      }
      return;
    }

    const dismissBtn = e.target.closest("[data-dismiss='modal']");
    if (dismissBtn) {
      e.preventDefault();
      const modal = dismissBtn.closest(".modal");
      if (modal) {
        hideModal(modal);
      }
      return;
    }

    if (e.target.classList.contains("modal")) {
      const modal = e.target;
      if (modal.getAttribute("data-backdrop") !== "static") {
        hideModal(modal);
      }
    }
  });

  document.addEventListener("keydown", e => {
    if (e.key === "Escape") {
      const openModals = document.querySelectorAll(".modal.in");
      if (openModals.length > 0) {
        const topModal = openModals[openModals.length - 1];
        if (topModal.getAttribute("data-keyboard") !== "false") {
          hideModal(topModal);
        }
      }
    }
  });
}

// Lightweight compatibility helper for existing tests or console scripts
function $(selector) {
  if (typeof selector === "function") {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", selector);
    } else {
      selector();
    }
    return;
  }

  let elements;
  if (typeof selector === "string") {
    elements = Array.from(document.querySelectorAll(selector));
  } else if (selector instanceof NodeList || Array.isArray(selector)) {
    elements = Array.from(selector);
  } else if (selector instanceof Node || selector === window || selector === document) {
    elements = [selector];
  } else {
    elements = [];
  }

  return {
    get length() {
      return elements.length;
    },
    [Symbol.iterator]: function* () {
      yield* elements;
    },
    text(value) {
      if (value !== undefined) {
        elements.forEach(el => (el.textContent = value));
        return this;
      }
      return elements.map(el => el.textContent || "").join("");
    },
    html(value) {
      if (value !== undefined) {
        elements.forEach(el => (el.innerHTML = value));
        return this;
      }
      return elements[0] ? elements[0].innerHTML : "";
    },
    val(value) {
      if (value !== undefined) {
        elements.forEach(el => (el.value = value));
        return this;
      }
      return elements[0] ? elements[0].value : "";
    },
    prop(name, value) {
      if (value !== undefined) {
        elements.forEach(el => (el[name] = value));
        return this;
      }
      return elements[0] ? elements[0][name] : undefined;
    },
    hasClass(className) {
      return elements.some(el => el.classList && el.classList.contains(className));
    },
    addClass(className) {
      elements.forEach(el => el.classList && el.classList.add(...className.split(/\s+/).filter(Boolean)));
      return this;
    },
    removeClass(className) {
      elements.forEach(el => el.classList && el.classList.remove(...className.split(/\s+/).filter(Boolean)));
      return this;
    },
    modal(action) {
      elements.forEach(el => {
        if (action === "show") showModal(el);
        else if (action === "hide") hideModal(el);
      });
      return this;
    },
    submit() {
      elements.forEach(el => {
        if (typeof el.requestSubmit === "function") {
          el.requestSubmit();
        } else if (typeof el.submit === "function") {
          el.submit();
        } else {
          el.dispatchEvent(new Event("submit", {bubbles: true, cancelable: true}));
        }
      });
      return this;
    },
    find(subSelector) {
      const matched = [];
      elements.forEach(el => {
        if (el.querySelectorAll) {
          matched.push(...el.querySelectorAll(subSelector));
        }
      });
      return $(matched);
    },
    each(fn) {
      elements.forEach((el, i) => fn.call(el, i, el));
      return this;
    },
    on(event, handler) {
      const events = event.split(/\s+/).filter(Boolean);
      elements.forEach(el => {
        events.forEach(ev => el.addEventListener(ev, handler));
      });
      return this;
    },
    click(handler) {
      if (handler) {
        elements.forEach(el => el.addEventListener("click", handler));
      } else {
        elements.forEach(el => el.click());
      }
      return this;
    },
    data(key) {
      return elements[0] && elements[0].dataset ? elements[0].dataset[key] : undefined;
    },
    css(prop, val) {
      if (val !== undefined) {
        elements.forEach(el => (el.style[prop] = val));
        return this;
      }
      return elements[0] ? window.getComputedStyle(elements[0])[prop] : undefined;
    },
  };
}

if (typeof window !== "undefined") {
  window.$ = $;
  if (typeof window.mdtoast !== "undefined") {
    $.mdtoast = window.mdtoast;
  }
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", main);
} else {
  main();
}

// noinspection JSUnusedGlobalSymbols
function onTurnstileSuccess(token) {
  turnstileToken = token;
}

function main() {
  setupSentry();
  setupElements();
  setupModalDelegation();
  setupPlaceload();
  setupEmojiConverter();
  initTaskAndBattles();
  setUiListeners();
  setupProlificSessionIfNeeded();
}

function setupSentry() {
  // The following key is public.
  window.sentryOnLoad = () =>
      Sentry.init({
        dsn: "https://85c805830c8feb48a02488382cecd1a5@o134888.ingest.us.sentry.io/4510338807627776",
        sendDefaultPii: true,
        release: "mwahahavote@0.1.0",
        denyUrls: [/https?:\/\/localhost/, /https?:\/\/127\.0\.0\.1/, /https?:\/\/\[::1?]/],
      });
}

function setupElements() {
  taskSelect = document.getElementById("task");
  promptText = document.getElementById("prompt-text");
  outputA = document.getElementById("output-a");
  outputAText = document.getElementById("output-a-text");
  outputB = document.getElementById("output-b");
  outputBText = document.getElementById("output-b-text");
  answerButtons = document.querySelectorAll("#answers button");
  isOffensiveLeft = document.getElementById("is-offensive-left");
  voteLeft = document.getElementById("vote-left");
  tie = document.getElementById("tie");
  skip = document.getElementById("skip");
  voteRight = document.getElementById("vote-right");
  isOffensiveRight = document.getElementById("is-offensive-right");
  prolificConsent = document.getElementById("prolific-consent");
  prolificVoteCountEl = document.getElementById("prolific-vote-count");
}

function fadeTransition(element, updateFn, duration = 100) {
  element.style.transition = `opacity ${duration}ms ease-in-out`;
  element.style.opacity = "0";
  setTimeout(() => {
    updateFn();
    element.style.opacity = "1";
  }, duration);
}

function showBattle() {
  const promptEl = document.getElementById("prompt");
  const outputsEl = document.getElementById("outputs");
  const answersEl = document.getElementById("answers");

  if (battles.length === 0) {
    if (promptEl) promptEl.style.display = "none";
    if (outputA) outputA.style.display = "none";
    if (outputB) outputB.style.display = "none";
    if (outputsEl) {
      const existingNotice = outputsEl.querySelector(".no-battles-notice");
      if (!existingNotice) {
        const notice = document.createElement("p");
        notice.className = "col-xs-12 no-battles-notice";
        notice.style.padding = "10px";
        notice.textContent = "There are no battles to display for this task.";
        outputsEl.appendChild(notice);
      }
    }
    if (answersEl) answersEl.style.display = "none";
  } else {
    fadeTransition(promptText, () => {
      const promptImageUrl = battles[index].prompt_image_url;
      let imageHtml = "";
      if (promptImageUrl) {
        imageHtml = '<img id="prompt-image" src="' + promptImageUrl + '" alt="Prompt image"><br/>';
      }
      promptText.innerHTML = imageHtml + emoji.replace_unified(battles[index].prompt || "").replace(/\n/mg, "<br/>");
    });
    fadeTransition(outputAText, () => {
      outputAText.innerHTML = emoji.replace_unified(battles[index].output_a.replace(/\n/mg, "<br/>"));
    });
    fadeTransition(outputBText, () => {
      outputBText.innerHTML = emoji.replace_unified(battles[index].output_b.replace(/\n/mg, "<br/>"));
    });
  }
}

function setupPlaceload() {
  Placeload.$("#prompt-text")
      .config({speed: "1s"})
      .line(element => element.width(100).height(15))
      .fold(
          () => {
          },
          () => {
          }
      );
  Placeload.$("#output-a-text")
      .config({speed: "1s"})
      .line(element => element.width(100).height(15))
      .config({spaceBetween: "7px"})
      .line(element => element.width(100).height(15))
      .config({spaceBetween: "7px"})
      .line(element => element.width(40).height(15))
      .fold(
          () => {
          },
          () => {
          }
      );
  Placeload.$("#output-b-text")
      .config({speed: "1s"})
      .line(element => element.width(100).height(15))
      .config({spaceBetween: "7px"})
      .line(element => element.width(100).height(15))
      .config({spaceBetween: "7px"})
      .line(element => element.width(40).height(15))
      .fold(
          () => {
          },
          () => {
          }
      );
}

function setupEmojiConverter() {
  // noinspection JSUnresolvedFunction
  emoji = new EmojiConvertor();
  emoji.img_set = "twitter";
  emoji.img_sets.twitter.path =
      "https://raw.githubusercontent.com/iamcal/emoji-data/a97b2d2efa64535d6300660eb2cd15ecb584e79e/img-twitter-64/";
}

function initTaskAndBattles() {
  fetch("api/config")
      .then(resp => {
        if (!resp.ok) throw new Error("Config response not ok");
        return resp.json();
      })
      .then(config => {
        const taskContainer = document.getElementById("task-container");
        if (config.tasks && config.tasks.length > 1) {
          taskSelect.innerHTML = "";
          config.tasks.forEach(t => {
            const opt = document.createElement("option");
            opt.value = t.id;
            opt.textContent = t.label;
            taskSelect.appendChild(opt);
          });
          if (taskContainer) taskContainer.style.display = "";
        } else {
          if (taskContainer) taskContainer.style.display = "none";
        }

        const urlParams = new URLSearchParams(window.location.search);
        const requestedTask = urlParams.get("task");
        const validTaskIds = config.tasks ? config.tasks.map(t => t.id) : [config.default_task];

        if (requestedTask && validTaskIds.includes(requestedTask)) {
          task = requestedTask;
        } else if (config.tasks && config.tasks.length > 1 && !requestedTask) {
          if (navigator.language.startsWith("es") && validTaskIds.includes("a-es")) {
            task = "a-es";
          } else if (navigator.language.startsWith("zh") && validTaskIds.includes("a-zh")) {
            task = "a-zh";
          } else {
            task = config.default_task;
          }
        } else {
          task = config.default_task || "a-en";
        }

        urlParams.set("task", task);
        taskSelect.value = task;

        if (config.tasks && config.tasks.length > 1) {
          const newUrl =
              window.location.protocol +
              "//" +
              window.location.host +
              window.location.pathname +
              "?" +
              urlParams.toString();
          window.history.replaceState({}, "", newUrl);
        }

        getBattles();
      })
      .catch(() => {
        getBattles();
      });
}

function getBattles() {
  fetch("battles?task=" + encodeURIComponent(task))
      .then(resp => {
        if (!resp.ok) throw new Error("Battles response not ok");
        return resp.json();
      })
      .then(data => {
        battles = data;
        showBattle();
      })
      .catch(err => {
        console.error("Error fetching battles:", err);
      });
}

function setUiListeners() {
  taskSelect.addEventListener("change", () => changeTask());

  answerButtons.forEach(btn => {
    btn.addEventListener("mouseup", () => {
      btn.blur();
      btn.classList.add("no-hover");
    });
    btn.addEventListener("mousemove", () => btn.classList.remove("no-hover"));
    btn.addEventListener("mousedown", () => btn.classList.remove("no-hover"));
  });

  voteLeft.addEventListener("click", () => vote("a"));
  tie.addEventListener("click", () => vote("t"));
  skip.addEventListener("click", () => vote("n"));
  voteRight.addEventListener("click", () => vote("b"));

  const cookieSettingsLink = document.getElementById("cookie-settings-link");
  if (cookieSettingsLink) {
    cookieSettingsLink.addEventListener("click", e => {
      e.preventDefault();
      window.showCookieSettings();
    });
  }
}

function changeTask() {
  window.location.href = "?task=" + encodeURIComponent(taskSelect.value);
}

function resetTurnstile() {
  if (typeof turnstile !== "undefined") {
    turnstile.reset();
  }
  turnstileToken = null;
}

function vote(voteOption) {
  const toastFn = window.mdtoast || (window.$ && window.$.mdtoast);
  if (!turnstileToken) {
    if (toastFn) {
      toastFn("Please complete the verification challenge (CAPTCHA)", {duration: 3000});
    }
    return;
  }

  const payload = new URLSearchParams({
    token: battles[index].token,
    vote: voteOption,
    is_offensive_a: isOffensiveLeft.checked.toString(),
    is_offensive_b: isOffensiveRight.checked.toString(),
    turnstile_token: turnstileToken,
  });

  fetch("vote", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    },
    body: payload.toString(),
  })
      .then(response => {
        if (!response.ok) {
          throw new Error("Vote post failed");
        }
        resetTurnstile();
      })
      .catch(() => {
        if (toastFn) {
          toastFn(translations[getLanguageFromTask()].error, {duration: 3000});
        }
        resetTurnstile();
      });

  index = (index + 1) % battles.length;

  const usedCount = (index - nextReplaceIndex + battles.length) % battles.length;
  const remainingCount = battles.length - usedCount;

  if (remainingCount <= Math.floor(battles.length / 2)) {
    const ignoredTokens = battles.map(battle => battle.token);
    const params = new URLSearchParams();
    params.set("task", task);
    params.set("batch_size", usedCount.toString());
    for (const tok of ignoredTokens) {
      params.append("ignored_tokens[]", tok);
    }

    fetch("battles?" + params.toString())
        .then(resp => {
          if (!resp.ok) throw new Error("Battles prefetch failed");
          return resp.json();
        })
        .then(data => {
          for (let i = 0; i < Math.min(data.length, usedCount); i++) {
            battles[nextReplaceIndex] = data[i];
            nextReplaceIndex = (nextReplaceIndex + 1) % battles.length;
          }
        })
        .catch(err => {
          console.error("Error prefetching battles:", err);
        });
  }

  showBattle();

  if (toastFn) {
    toastFn(toastText(voteOption), {duration: 3000});
  }

  isOffensiveLeft.checked = false;
  isOffensiveRight.checked = false;

  if (isAProlificSession && voteOption !== "n") {
    prolificVoteCount++;
    updateProlificVoteCount();
  }
}

function getLanguageFromTask() {
  return task.startsWith("a-") ? task.split("-")[1] : "en";
}

function translateModal(lang) {
  document.querySelectorAll("[data-i18n]").forEach(el => {
    const key = el.getAttribute("data-i18n");
    const keys = key.split(".");
    let value = translations[lang];
    for (let i = 0; i < keys.length; i++) {
      value = value ? value[keys[i]] : undefined;
    }
    if (value !== undefined) {
      el.textContent = value;
    }
  });

  document.querySelectorAll("[data-i18n-html]").forEach(el => {
    const key = el.getAttribute("data-i18n-html");
    const keys = key.split(".");
    let value = translations[lang];
    for (let i = 0; i < keys.length; i++) {
      value = value ? value[keys[i]] : undefined;
    }
    if (value !== undefined) {
      el.innerHTML = value;
    }
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
  if (prolificVoteCount === PROLIFIC_MAX_BATTLES) {
    const prolificTask = document.getElementById("prolific-task");
    if (prolificTask) {
      prolificTask.value = task;
    }
    showModal(document.getElementById("prolific-finished"));
  } else {
    prolificVoteCount %= PROLIFIC_MAX_BATTLES;
    const lang = getLanguageFromTask();
    const t = translations[lang];
    prolificVoteCountEl.textContent = `${t.progress}: ${prolificVoteCount}/${PROLIFIC_MAX_BATTLES}`;
  }
}

function setupProlificSessionIfNeeded() {
  if (isAProlificSession) {
    const lang = getLanguageFromTask();

    translateModal(lang);

    const consentForm = prolificConsent.querySelector("form");
    if (consentForm) {
      consentForm.addEventListener("submit", e => {
        localStorage.setItem(`consent-prolific-id-${prolificId}`, "done");
        hideModal(prolificConsent);
        showModal(document.getElementById("helpModal"));
        e.preventDefault();
        e.stopPropagation();
      });
    }

    taskSelect.disabled = true;

    if (localStorage.getItem(`consent-prolific-id-${prolificId}`) !== "done") {
      const prolificIdInput = document.getElementById("prolific-id");
      if (prolificIdInput) {
        prolificIdInput.value = prolificId;
      }
      showModal(prolificConsent);
      const consentContinueBtn = document.getElementById("prolific-consent-continue");
      if (consentContinueBtn) {
        consentContinueBtn.addEventListener("click", () => {
          fetch("prolific-consent", {method: "POST"});
        });
      }
    }

    fetch("session-vote-count")
        .then(resp => {
          if (!resp.ok) throw new Error("session-vote-count failed");
          return resp.json();
        })
        .then(count => {
          prolificVoteCount = count;
          updateProlificVoteCount();
          prolificVoteCountEl.style.display = "block";
        })
        .catch(err => {
          console.error("Error fetching session vote count:", err);
        });
  }
}

/**
 * Cookie Consent Management.
 * Handles cookie consent banner display and user preferences.
 */
(() => {
  "use strict";

  const CONSENT_KEY = "cookie-consent-v1";
  const GA_TRACKING_ID = "G-40699CQRM6";

  /**
   * Get stored consent preference from localStorage.
   * @returns {Object|null} Consent object or null if not set
   */
  function getConsentPreference() {
    try {
      const stored = localStorage.getItem(CONSENT_KEY);
      return stored ? JSON.parse(stored) : null;
    } catch (e) {
      console.error("Error reading consent preference:", e);
      return null;
    }
  }

  /**
   * Save consent preference to localStorage.
   * @param {boolean} analytics - Whether user accepts analytics cookies
   */
  function saveConsentPreference(analytics) {
    try {
      localStorage.setItem(
          CONSENT_KEY,
          JSON.stringify({essential: true, analytics: analytics, timestamp: Date.now()})
      );
    } catch (e) {
      console.error("Error saving consent preference:", e);
    }
  }

  /**
   * Apply the consent preferences (enable/disable analytics).
   * @param {Object} preferences - Consent preferences object
   */
  function applyConsent(preferences) {
    window["ga-disable-" + GA_TRACKING_ID] = !preferences.analytics;
    if (typeof gtag === "function") {
      gtag("consent", "update", {analytics_storage: preferences.analytics ? "granted" : "denied"});
    }
  }

  /**
   * Create and show the cookie consent banner.
   */
  function showBanner() {
    // Remove the existing banner if present.
    const existing = document.getElementById("cookie-consent-banner");
    if (existing) {
      existing.remove();
    }

    // Create banner HTML.
    const banner = document.createElement("div");
    banner.id = "cookie-consent-banner";
    banner.className = "cookie-consent-banner";
    banner.innerHTML = `
      <div class="cookie-banner-content">
        <div class="cookie-banner-row">
          <div class="cookie-banner-text">
            <h4>We Value Your Privacy</h4>
            <p>
              This website uses cookies for essential functionality (annotation session tracking) and optional analytics.
              You can choose which to accept.
            </p>
          </div>
          <div class="cookie-banner-buttons">
            <button id="cookie-accept-all">Accept All</button>
            <button id="cookie-essential-only">Essential Only</button>
          </div>
        </div>
      </div>
    `;

    // Append to body.
    document.body.appendChild(banner);

    // Trigger slide-up animation after a brief delay.
    setTimeout(() => banner.classList.add("show"), 100);

    // Attach event listeners.
    document.getElementById("cookie-accept-all").addEventListener("click", acceptAllCookies);
    document.getElementById("cookie-essential-only").addEventListener("click", acceptEssentialOnly);
  }

  /**
   * Hide and remove the banner with animation.
   */
  function hideBanner() {
    const banner = document.getElementById("cookie-consent-banner");
    if (banner) {
      banner.classList.remove("show");
      setTimeout(() => banner.remove(), 300);
    }
  }

  /**
   * Handle the "Accept All" button click.
   */
  function acceptAllCookies() {
    saveConsentPreference(true);
    applyConsent({analytics: true});
    hideBanner();

    // Reload analytics if it was previously disabled.
    reloadAnalyticsIfNeeded();
  }

  /**
   * Handle the "Essential Only" button click.
   */
  function acceptEssentialOnly() {
    saveConsentPreference(false);
    applyConsent({analytics: false});
    hideBanner();
  }

  /**
   * Reload analytics if the user accepts after previously declining.
   */
  function reloadAnalyticsIfNeeded() {
    // Only reload if we're not on localhost and gtag is not yet loaded.
    if (document.location.hostname === "localhost") {
      return;
    }

    // If gtag doesn't exist yet, load it
    if (typeof gtag === "undefined") {
      const script = document.createElement("script");
      script.async = true;
      script.src = "https://www.googletagmanager.com/gtag/js?id=" + GA_TRACKING_ID;
      document.head.appendChild(script);

      script.onload = () => {
        window.dataLayer = window.dataLayer || [];

        function gtag() {
          dataLayer.push(arguments);
        }

        gtag("js", new Date());
        gtag("config", GA_TRACKING_ID);
      };
    }
  }

  /**
   * Initialize the cookie consent on page load.
   */
  function initCookieConsent() {
    const consent = getConsentPreference();
    if (consent) {
      applyConsent(consent);
    } else {
      setTimeout(showBanner, 500); // First visit - show banner after a slight delay
    }
  }

  /**
   * Expose the function to show the cookie settings (for the footer link).
   */
  window.showCookieSettings = () => showBanner();

  // Initialize when DOM is ready
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initCookieConsent);
  } else {
    initCookieConsent();
  }
})();
