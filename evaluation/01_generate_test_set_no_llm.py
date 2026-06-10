"""
Rule-based test set generator — NO LLM required.

Uses:
  - 60 hardcoded academic Q&A pairs (definition / comparison / multi_step)
  - 20 find_material questions paired with real ES document UIDs
  - 20 hardcoded out-of-scope questions

Run:
  python3 01_generate_test_set_no_llm.py [--force]

  --force   Overwrite existing test_set.json
"""

import argparse
import json
import logging
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from config import ES_HOST, ES_INDEX, TEST_SET_PATH

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Hardcoded in-scope questions  (definition, comparison, multi_step)
# ---------------------------------------------------------------------------

_DEFINITION_QA: list[tuple[str, str]] = [
    (
        "What is opportunity cost in economics?",
        "Opportunity cost is the value of the next-best alternative forgone when making a choice. It represents the trade-off inherent in every economic decision.",
    ),
    (
        "What is a derivative in calculus?",
        "A derivative measures the instantaneous rate of change of a function with respect to one of its variables. Geometrically it is the slope of the tangent line to the function's graph at a given point.",
    ),
    (
        "What is photosynthesis?",
        "Photosynthesis is the biological process by which plants, algae, and some bacteria use sunlight, water, and carbon dioxide to produce glucose and oxygen. It occurs primarily in chloroplasts.",
    ),
    (
        "What is the Central Limit Theorem?",
        "The Central Limit Theorem states that the distribution of sample means approaches a normal distribution as sample size increases, regardless of the population's original distribution, given a sufficiently large sample.",
    ),
    (
        "What is object-oriented programming?",
        "Object-oriented programming (OOP) is a programming paradigm that organises code into objects—data structures that bundle state (attributes) and behaviour (methods)—and uses principles like encapsulation, inheritance, and polymorphism.",
    ),
    (
        "What is GDP (Gross Domestic Product)?",
        "GDP is the total monetary value of all final goods and services produced within a country's borders during a specific time period. It is the primary measure of a nation's economic output and size.",
    ),
    (
        "What is natural selection?",
        "Natural selection is the mechanism of evolution proposed by Darwin in which organisms with heritable traits better suited to their environment tend to survive and reproduce more successfully, passing those traits to offspring.",
    ),
    (
        "What is a null hypothesis in statistics?",
        "The null hypothesis (H0) is a default statement that there is no significant effect or relationship between variables. Statistical tests are designed to evaluate whether there is enough evidence to reject it.",
    ),
    (
        "What is Newton's second law of motion?",
        "Newton's second law states that the net force acting on an object equals the product of its mass and acceleration (F = ma). It describes how the velocity of an object changes when it is subjected to an external force.",
    ),
    (
        "What is a chemical bond?",
        "A chemical bond is the force of attraction that holds two or more atoms together in a compound. The main types are covalent bonds (shared electrons), ionic bonds (electron transfer), and metallic bonds.",
    ),
    (
        "What is an algorithm in computer science?",
        "An algorithm is a finite, well-defined sequence of instructions for solving a computational problem or accomplishing a task. Algorithms are characterised by their correctness, efficiency, and clarity.",
    ),
    (
        "What is market equilibrium?",
        "Market equilibrium is the state in which the quantity of a good supplied equals the quantity demanded at a particular price level. At equilibrium, there is no tendency for price to change.",
    ),
    (
        "What is entropy in thermodynamics?",
        "Entropy is a thermodynamic quantity that measures the degree of disorder or randomness in a system. The second law of thermodynamics states that entropy in an isolated system tends to increase over time.",
    ),
    (
        "What is a polynomial?",
        "A polynomial is a mathematical expression consisting of variables and coefficients combined using addition, subtraction, and multiplication, with non-negative integer exponents. Examples include linear (degree 1) and quadratic (degree 2) expressions.",
    ),
    (
        "What is aggregate demand in macroeconomics?",
        "Aggregate demand is the total demand for all goods and services in an economy at a given price level and time. Its components are consumption, investment, government spending, and net exports (C + I + G + NX).",
    ),
    (
        "What is a variable in programming?",
        "A variable is a named storage location in memory that holds a value which can change during program execution. Variables have a name, a data type, and a value assigned by the programmer.",
    ),
    (
        "What is a confidence interval in statistics?",
        "A confidence interval is a range of values, derived from sample data, that is likely to contain the true population parameter with a specified probability (confidence level), such as 95%.",
    ),
    (
        "What is magnetic flux?",
        "Magnetic flux is the measure of the total magnetic field passing through a given area. It is calculated as the dot product of the magnetic field vector and the area vector, and its SI unit is the Weber (Wb).",
    ),
    (
        "What is organisational culture?",
        "Organisational culture is the shared set of values, beliefs, norms, and practices that shape how members of an organisation interact and work together. It influences behaviour, decision-making, and the overall work environment.",
    ),
    (
        "What is a hypothesis in scientific research?",
        "A hypothesis is a testable, falsifiable prediction about the relationship between variables, made before conducting an experiment. It provides the basis for designing experiments and interpreting results.",
    ),
]

_COMPARISON_QA: list[tuple[str, str]] = [
    (
        "What is the difference between supervised and unsupervised machine learning?",
        "Supervised learning trains on labelled data to predict outputs for new inputs, while unsupervised learning finds patterns in unlabelled data without predefined output categories. Supervised tasks include classification and regression; unsupervised tasks include clustering and dimensionality reduction.",
    ),
    (
        "How does kinetic energy differ from potential energy?",
        "Kinetic energy is the energy an object possesses due to its motion (KE = half mv squared), while potential energy is stored energy due to position or configuration (e.g. gravitational PE = mgh). Energy converts between the two forms as objects move.",
    ),
    (
        "What is the difference between macroeconomics and microeconomics?",
        "Microeconomics studies individual decision-making by households and firms and how markets allocate resources. Macroeconomics examines the economy as a whole—national output, unemployment, inflation, and fiscal or monetary policy.",
    ),
    (
        "How does a stack differ from a queue as a data structure?",
        "A stack follows Last-In-First-Out (LIFO) order, where the last element inserted is the first removed. A queue follows First-In-First-Out (FIFO) order, where the first element inserted is the first removed.",
    ),
    (
        "What is the difference between an acid and a base in chemistry?",
        "An acid donates protons (H+ ions) in solution and has a pH below 7, while a base accepts protons and has a pH above 7. Together they neutralise each other to form water and a salt.",
    ),
    (
        "How does correlation differ from causation in statistics?",
        "Correlation measures the statistical association between two variables without implying direction of cause. Causation means that a change in one variable directly produces a change in the other, which requires controlled experimentation to establish.",
    ),
    (
        "What is the difference between amplitude and frequency in wave physics?",
        "Amplitude is the maximum displacement of a wave from its equilibrium position, determining intensity or loudness. Frequency is the number of complete wave cycles per second (measured in Hz), determining pitch or colour.",
    ),
    (
        "How does mitosis differ from meiosis in cell division?",
        "Mitosis produces two genetically identical diploid daughter cells for growth and repair. Meiosis produces four genetically diverse haploid cells (gametes) and involves two sequential division stages, enabling sexual reproduction.",
    ),
    (
        "What is the difference between inductive and deductive reasoning?",
        "Deductive reasoning draws logically certain conclusions from general premises. Inductive reasoning generalises from specific observations to probable conclusions; the conclusions are probable but not guaranteed.",
    ),
    (
        "What is the difference between fiscal policy and monetary policy?",
        "Fiscal policy involves government decisions on taxation and spending to influence the economy. Monetary policy is managed by a central bank and controls the money supply and interest rates to achieve macroeconomic goals.",
    ),
    (
        "How does a conductor differ from a semiconductor in electronics?",
        "A conductor has low electrical resistance and allows current to flow freely (e.g. copper). A semiconductor has intermediate resistivity that can be controlled by doping or temperature (e.g. silicon), making it the basis of transistors and integrated circuits.",
    ),
    (
        "What is the difference between a function and a method in object-oriented programming?",
        "A function is a standalone block of reusable code defined outside any class. A method is a function defined inside a class and is associated with an object's state; it is called on an instance using dot notation.",
    ),
    (
        "How does static typing differ from dynamic typing in programming languages?",
        "In statically typed languages, variable types are checked at compile time, catching type errors early (e.g. Java, C++). In dynamically typed languages, types are checked at runtime, offering flexibility at the cost of later error detection (e.g. Python, JavaScript).",
    ),
    (
        "What is the difference between DNA and RNA?",
        "DNA is double-stranded, contains deoxyribose sugar and thymine, and stores genetic information long-term. RNA is single-stranded, contains ribose sugar and uracil, and serves as a messenger for protein synthesis (mRNA) or structural/catalytic roles (rRNA, tRNA).",
    ),
    (
        "How does a market economy differ from a planned economy?",
        "In a market economy, prices and production are determined by supply and demand decisions of private actors. In a planned (command) economy, the government centrally decides resource allocation, production targets, and prices.",
    ),
    (
        "What is the difference between encryption and hashing?",
        "Encryption is a reversible process that transforms data using a key so it can be decrypted later. Hashing is a one-way function that maps data to a fixed-size digest with no practical way to reverse it; it is used for integrity verification and password storage.",
    ),
    (
        "How does classical conditioning differ from operant conditioning in psychology?",
        "Classical conditioning (Pavlov) pairs a neutral stimulus with an unconditioned stimulus until the neutral stimulus alone elicits a response. Operant conditioning (Skinner) shapes voluntary behaviour through reinforcement (rewarding desired behaviour) and punishment.",
    ),
    (
        "What is the difference between a primary source and a secondary source in historical research?",
        "A primary source is an original, first-hand account or artefact from the time period studied (e.g. diaries, government documents). A secondary source analyses, interprets, or summarises primary sources (e.g. textbooks, scholarly articles).",
    ),
    (
        "How does a hypothesis differ from a theory in science?",
        "A hypothesis is an initial, testable prediction about a phenomenon, not yet supported by extensive evidence. A scientific theory is a well-substantiated explanation supported by a large body of repeated experiments, observations, and predictive success.",
    ),
    (
        "What is the difference between procedural programming and object-oriented programming?",
        "Procedural programming organises code as sequences of instructions (procedures/functions) that operate on data passed to them. Object-oriented programming bundles data and the functions that operate on it into objects, emphasising encapsulation, inheritance, and polymorphism.",
    ),
]

_MULTI_STEP_QA: list[tuple[str, str]] = [
    (
        "How does natural selection drive evolutionary change over generations?",
        "Natural selection operates through variation (individuals differ in heritable traits), differential survival (some traits improve fitness), reproduction (survivors pass traits to offspring), and accumulation (advantageous traits become more common). Over many generations this produces adaptation and speciation.",
    ),
    (
        "What are the steps of the scientific method?",
        "The scientific method proceeds through observation (noticing a phenomenon), question formulation, hypothesis generation (testable prediction), experimental design, data collection, analysis, conclusion, and peer-reviewed publication. Results are replicated by others to confirm validity.",
    ),
    (
        "How does the Federal Reserve use interest rates to control inflation?",
        "When inflation rises, the Fed raises the federal funds rate, which increases borrowing costs, reduces consumer and business spending, cools aggregate demand, and slows price increases. Lowering rates does the reverse to stimulate a sluggish economy.",
    ),
    (
        "How does TCP/IP ensure reliable data transmission over a network?",
        "TCP breaks data into packets, adds sequence numbers, sends them over IP routing, and at the destination reassembles them. An acknowledgement (ACK) mechanism detects lost packets, which are retransmitted. Flow control and congestion control prevent network overload.",
    ),
    (
        "How does photosynthesis convert sunlight into chemical energy?",
        "In the light-dependent reactions in the thylakoid membranes, sunlight splits water molecules, releasing oxygen and generating ATP and NADPH. In the Calvin cycle in the stroma, these energy carriers drive the fixation of CO2 into glucose.",
    ),
    (
        "What are the main stages of the software development lifecycle (SDLC)?",
        "A typical SDLC moves through requirements analysis, system design, implementation (coding), testing (unit, integration, system), deployment, and maintenance. Agile variants iterate through these stages in short sprints rather than sequentially.",
    ),
    (
        "How does a recursive algorithm compute a factorial?",
        "factorial(n) = n multiplied by factorial(n-1) with base case factorial(0) = 1. Each recursive call reduces n by 1, building a call stack until the base case is reached, then returns are multiplied back up the stack to yield the final product.",
    ),
    (
        "Explain how DNA replication ensures genetic continuity.",
        "Helicase unwinds the double helix; primase lays an RNA primer; DNA polymerase synthesises new complementary strands (leading strand continuously, lagging strand in Okazaki fragments); ligase joins fragments; proofreading enzymes correct errors—each daughter cell inherits one original and one new strand.",
    ),
    (
        "How does a market reach equilibrium according to supply and demand theory?",
        "If price is above equilibrium, surplus exists—producers cut prices to sell excess stock; if below, shortage exists—prices rise. These pressures move price toward the point where quantity supplied equals quantity demanded, clearing the market.",
    ),
    (
        "How does merge sort achieve O(n log n) time complexity?",
        "Merge sort recursively divides the array in half (log n splits), then merges sorted halves in O(n) time per level. Because there are log n levels each requiring O(n) work, total complexity is O(n log n), which is asymptotically optimal for comparison-based sorting.",
    ),
    (
        "How does the human immune system respond to a new viral infection?",
        "Innate immunity provides immediate non-specific defence (phagocytes, inflammation). Dendritic cells present viral antigens to T-cells; helper T-cells activate B-cells to produce antibodies and cytotoxic T-cells to kill infected cells. Memory B and T cells persist for faster future responses.",
    ),
    (
        "What are the key steps in conducting a statistical hypothesis test?",
        "Define null and alternative hypotheses, choose a significance level (alpha), select an appropriate test statistic, calculate the test statistic from sample data, determine the p-value, compare p-value to alpha, reject or fail to reject H0, then interpret results in context.",
    ),
    (
        "How does object-oriented inheritance promote code reuse?",
        "A child class inherits attributes and methods from a parent class without rewriting them. It can override methods for specialised behaviour (polymorphism) and add new attributes. This hierarchy means shared logic lives in one place, reducing duplication and easing maintenance.",
    ),
    (
        "How do supply shocks affect macroeconomic output and prices?",
        "A negative supply shock (e.g. oil price spike) shifts the short-run aggregate supply curve left, raising the price level and reducing output simultaneously (stagflation). Policy must choose between fighting inflation (tighten) or supporting output (loosen), as both goals conflict.",
    ),
    (
        "How do neurons transmit signals in the nervous system?",
        "A neuron receives chemical signals at dendrites, which generate an electrical action potential if the threshold is exceeded. The action potential travels down the axon via ion channel propagation, triggers neurotransmitter release at the synaptic terminal, and these chemicals bind receptors on the next neuron.",
    ),
    (
        "What are the main phases of a project management lifecycle?",
        "Projects move through initiation (define scope and feasibility), planning (schedule, budget, resources), execution (team does the work), monitoring and controlling (track progress against plan), and closure (deliver output, document lessons learned, release resources).",
    ),
    (
        "How does caching improve application performance?",
        "Frequently accessed data is stored in a fast-access cache (memory or CDN) so subsequent requests are served without hitting the slower database or origin server. Cache-hit ratio, eviction policies (LRU, LFU), and cache invalidation strategies determine effectiveness.",
    ),
    (
        "Explain how Kirchhoff's laws govern current and voltage in circuits.",
        "KCL (Current Law): the total current entering a node equals the total current leaving it—charge conservation. KVL (Voltage Law): the sum of voltage rises and drops around any closed loop equals zero—energy conservation. Together they give a complete system of equations for solving any resistive network.",
    ),
    (
        "How does the cell membrane regulate what enters and exits a cell?",
        "The phospholipid bilayer is selectively permeable: small non-polar molecules diffuse freely; ions and polar molecules cross via specific channel or carrier proteins (facilitated diffusion or active transport). Osmosis moves water across aquaporins down its concentration gradient.",
    ),
    (
        "Explain the relationship between consumer spending and the income multiplier effect.",
        "An initial injection of spending (e.g. government investment) becomes income for workers who spend a fraction (marginal propensity to consume, MPC) of it; recipients do the same. Total GDP increase = initial spending times 1/(1 minus MPC). Higher MPC produces a larger multiplier.",
    ),
]

# ---------------------------------------------------------------------------
# Hardcoded OOS questions
# ---------------------------------------------------------------------------
_OOS_QA: list[tuple[str, str]] = [
    ("What is a good recipe for making beef pho?", "This is a cooking question and not covered by academic OER materials."),
    ("How many calories are in a Big Mac?", "This is a nutritional fact about a fast-food product and not in scope for academic content."),
    ("What was the final score of yesterday's Champions League match?", "Live sports scores are not part of the OER academic corpus."),
    ("Can you recommend the best Netflix series to watch this weekend?", "Streaming entertainment recommendations are out of scope."),
    ("How do I lose weight quickly?", "Personal fitness and diet advice is not covered by OER course materials."),
    ("What is the current stock price of Apple Inc.?", "Real-time financial market data is not available in the academic corpus."),
    ("Write me a birthday poem for my friend.", "Creative writing requests are not in scope for this academic assistant."),
    ("Who is the current Prime Minister of Vietnam?", "Current political news is outside the scope of academic OER resources."),
    ("How do I create a TikTok account?", "Social media setup instructions are not part of OER course content."),
    ("What are the symptoms of the common cold?", "Personal medical advice is not covered by the academic materials."),
    ("Should I invest in Bitcoin or Ethereum right now?", "Cryptocurrency investment advice is out of scope for academic OER content."),
    ("What are the best tourist attractions in Paris?", "Travel recommendations are not part of the OER academic corpus."),
    ("How do I train my dog to sit?", "Pet training instructions are outside the scope of academic materials."),
    ("What are the visa requirements for visiting Japan?", "Travel visa information is not covered by academic OER resources."),
    ("How do I get a driver's licence in Vietnam?", "Government licence application procedures are out of scope."),
    ("What is the price of gold today?", "Real-time commodity prices are not part of the academic corpus."),
    ("Can you write lyrics for a pop song about summer?", "Creative music writing is not within scope of OER academic content."),
    ("What are the best laptops to buy for gaming?", "Consumer electronics purchasing advice is out of scope."),
    ("How do I book a flight to Singapore?", "Travel booking assistance is not part of the academic materials."),
    ("What diet should I follow to build muscle fast?", "Personal nutrition and bodybuilding advice is outside the OER academic scope."),
]

# ---------------------------------------------------------------------------
# Find-material: query ES at runtime, match to topic templates
# ---------------------------------------------------------------------------

_FM_SOURCES: list[tuple[str, str, str]] = [
    # (title_fragment_to_match, question, ground_truth)
    ("Organizational Behavior", "What academic resource provides an introduction to organisational behaviour?", "A textbook on Organisational Behavior covers topics such as motivation, team dynamics, leadership, and organisational culture."),
    ("Introduction to Business", "Where can I find an open-access introductory business textbook?", "An introductory business textbook covers foundational topics including management, marketing, accounting, and entrepreneurship."),
    ("College Algebra", "What academic resource covers college-level algebra topics?", "A college algebra textbook covers polynomial, rational, exponential, and logarithmic functions as well as systems of equations."),
    ("Elementary Algebra", "What open textbook can I use to learn elementary algebra?", "An elementary algebra textbook introduces variables, linear equations, inequalities, and basic graphing techniques."),
    ("Prealgebra", "What learning materials are available for pre-algebra?", "A pre-algebra textbook builds arithmetic foundations including fractions, decimals, ratios, and introductory equations."),
    ("Concepts of Biology", "What open-access resource introduces the fundamental concepts of biology?", "A concepts of biology textbook introduces cell theory, genetics, evolution, ecology, and the diversity of life."),
    ("Introduction to Anthropology", "What academic resource covers the fundamentals of anthropology?", "An anthropology textbook introduces cultural, biological, archaeological, and linguistic anthropology perspectives."),
    ("Precalculus", "What academic materials are available for learning precalculus?", "A precalculus textbook covers functions, trigonometry, analytic geometry, and sequences to prepare students for calculus."),
    ("Business Ethics", "Where can I find an open textbook on business ethics?", "A business ethics textbook examines ethical decision-making, corporate responsibility, stakeholder theory, and professional codes of conduct."),
    ("U.S. History", "What open educational resource covers United States history?", "A U.S. history textbook surveys American history from pre-Columbian civilisations through modern political and social developments."),
    ("Introduction to Python Programming", "What resource can help me learn Python programming from scratch?", "An introductory Python programming textbook teaches syntax, data types, control flow, functions, and introductory data structures."),
    ("Introduction to Sociology", "What open textbook introduces the field of sociology?", "A sociology textbook covers social institutions, culture, stratification, race, gender, and social change through a sociological lens."),
    ("Medical-Surgical Nursing", "What resource covers medical-surgical nursing practice?", "A medical-surgical nursing textbook covers assessment, evidence-based care, and management of common adult medical and surgical conditions."),
    ("College Success", "What academic resource provides guidance on succeeding in college?", "A college success textbook offers strategies for time management, note-taking, study skills, goal-setting, and academic integrity."),
    ("Fundamentals of Nursing", "What open textbook provides an introduction to nursing fundamentals?", "A fundamentals of nursing textbook covers patient assessment, clinical reasoning, basic care skills, and the nursing process."),
    ("World History", "What academic materials are available for studying world history?", "A world history textbook surveys global civilisations, trade networks, political systems, and cultural exchange from antiquity to the early modern period."),
    ("Principles of Macroeconomics", "What resource introduces macroeconomic principles?", "A macroeconomics textbook covers national income accounting, fiscal and monetary policy, inflation, unemployment, and economic growth."),
    ("University Physics", "What open textbook covers university-level physics concepts?", "A university physics textbook covers mechanics, electromagnetism, thermodynamics, optics, and modern physics at the introductory university level."),
    ("Intermediate Algebra", "Where can I find an intermediate algebra textbook?", "An intermediate algebra textbook extends elementary algebra to rational expressions, radical equations, quadratic functions, and conic sections."),
    ("Principles of Microeconomics", "What academic resource explains microeconomic principles?", "A microeconomics textbook covers consumer theory, production, market structures, externalities, and public goods."),
]


def _fetch_find_material_docs() -> list[dict]:
    """Query ES for openstax and mit_ocw docs to pair with find_material questions."""
    try:
        r = requests.post(
            f"{ES_HOST}/{ES_INDEX}/_search",
            json={
                "size": 100,
                "query": {"bool": {"must": [
                    {"exists": {"field": "asset_uid"}},
                    {"bool": {"should": [
                        {"term": {"source_system": "openstax"}},
                        {"term": {"source_system": "mit_ocw"}},
                    ]}},
                ]}},
                "_source": ["asset_uid", "title", "source_system", "url"],
            },
            timeout=10,
        )
        hits = r.json()["hits"]["hits"]
        return [h["_source"] for h in hits
                if h["_source"].get("asset_uid") and h["_source"]["asset_uid"] != "None"]
    except Exception as exc:
        log.warning("ES query failed (%s) — find_material will have null source_asset_uid", exc)
        return []


def _build_find_material(docs: list[dict]) -> list[dict]:
    title_to_doc: dict[str, dict] = {d["title"]: d for d in docs}
    used: set[str] = set()
    questions: list[dict] = []

    for fragment, question, ground_truth in _FM_SOURCES:
        matched: dict | None = None
        for title, doc in title_to_doc.items():
            if fragment.lower() in title.lower() and title not in used:
                matched = doc
                used.add(title)
                break

        questions.append({
            "question": question,
            "ground_truth": ground_truth,
            "source_asset_uid": matched["asset_uid"] if matched else None,
            "source_title": matched["title"] if matched else "Unknown",
            "source_system": matched.get("source_system", "") if matched else "",
            "source_url": matched.get("url", "") if matched else "",
            "question_type": "find_material",
            "difficulty": "easy",
            "language": "en",
        })

    return questions[:20]


# ---------------------------------------------------------------------------
# Assemble and write
# ---------------------------------------------------------------------------

def build_test_set() -> list[dict]:
    log.info("Fetching find_material document pairings from ES ...")
    docs = _fetch_find_material_docs()
    log.info("  ES returned %d usable docs", len(docs))

    records: list[dict] = []

    for q, gt in _DEFINITION_QA:
        records.append({"question": q, "ground_truth": gt, "source_asset_uid": None,
                        "source_title": "", "source_system": "", "source_url": "",
                        "question_type": "definition", "difficulty": "medium", "language": "en"})

    for q, gt in _COMPARISON_QA:
        records.append({"question": q, "ground_truth": gt, "source_asset_uid": None,
                        "source_title": "", "source_system": "", "source_url": "",
                        "question_type": "comparison", "difficulty": "medium", "language": "en"})

    for q, gt in _MULTI_STEP_QA:
        records.append({"question": q, "ground_truth": gt, "source_asset_uid": None,
                        "source_title": "", "source_system": "", "source_url": "",
                        "question_type": "multi_step", "difficulty": "hard", "language": "en"})

    records.extend(_build_find_material(docs))

    for q, gt in _OOS_QA:
        records.append({"question": q, "ground_truth": gt, "source_asset_uid": None,
                        "source_title": "", "source_system": "", "source_url": "",
                        "question_type": "out_of_scope", "difficulty": "easy", "language": "en"})

    for i, rec in enumerate(records, start=1):
        rec["id"] = f"Q{i:03d}"

    return records


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate test_set.json without any LLM.")
    parser.add_argument("--force", action="store_true", help="Overwrite existing test_set.json")
    args = parser.parse_args()

    if TEST_SET_PATH.exists() and not args.force:
        log.error("%s already exists. Use --force to overwrite.", TEST_SET_PATH)
        sys.exit(1)

    test_set = build_test_set()

    from collections import Counter
    counts = Counter(q["question_type"] for q in test_set)
    log.info("Question counts: %s  (total=%d)", dict(counts), len(test_set))

    TEST_SET_PATH.write_text(json.dumps(test_set, ensure_ascii=False, indent=2))
    log.info("Saved %d questions -> %s", len(test_set), TEST_SET_PATH)


if __name__ == "__main__":
    main()
