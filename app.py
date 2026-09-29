import streamlit as st
import os
import json
from gtts import gTTS
from dotenv import load_dotenv
from langchain_postgres import PGVector
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.chains import create_history_aware_retriever, create_retrieval_chain
from langchain.retrievers import ContextualCompressionRetriever
from langchain.retrievers.document_compressors import CrossEncoderReranker
from langchain_community.cross_encoders import HuggingFaceCrossEncoder
from langchain.chains.combine_documents import create_stuff_documents_chain
from embedder import EMBED_DIM, EMBED_MODEL, PrefixedEmbeddings
from pgurl import database_url, psycopg_dsn

# Load environment variables from .env file
load_dotenv()

# Corpus source. ingest.py fetches the public JSON API and mirrors it into
# PostgreSQL + pgvector; this app only ever reads that collection.
API_BASE = os.environ.get("API_BASE_URL", "https://bhumipedia.land.gov.bd").rstrip("/")
COLLECTION = os.environ.get("PG_COLLECTION", "land_acts")
INDEX_STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index_state.json")

# Initialize Groq for answering
groq_api_key = os.getenv('GROQ_API_KEY')
if not groq_api_key:
    st.sidebar.error("GROQ_API_KEY is not set. Please set it in the .env file.")
    st.stop()

model = 'qwen/qwen3.8-27b'

groq_chat = ChatGroq(
    groq_api_key=groq_api_key, 
    model=model,
    # Groq's on-demand tier caps output at 1000 tokens/minute; without an
    # explicit cap the request is sized by the model and rejected with a 429.
    max_tokens=800,
)

#embeddings = FastEmbedEmbeddings(model_name="BAAI/bge-base-en-v1.5")
# Same wrapper ingest.py used, so the index-time and query-time text prefixes
# and normalisation always match.
embeddings = PrefixedEmbeddings(EMBED_MODEL)


# Open the pre-built pgvector collection. Cached so the model and store load once.
@st.cache_resource(show_spinner="Connecting to the pgvector collection...")
def get_vector_store():
    if not database_url():
        return None
    try:
        return PGVector(
            embeddings=embeddings,
            connection=database_url(),
            embedding_length=EMBED_DIM,
            collection_name=COLLECTION,
            distance_strategy="cosine",
            use_jsonb=True,
            create_extension=True,
        )
    except Exception as exc:
        st.sidebar.error(f"Could not reach the vector store: {type(exc).__name__}: {exc}")
        return None


@st.cache_data(ttl=60, show_spinner=False)
def chunk_count(collection: str) -> int:
    """Live row count, since pgvector has no in-process index handle."""
    import psycopg

    try:
        with psycopg.connect(psycopg_dsn()) as conn:
            return conn.execute(
                "SELECT count(*) FROM langchain_pg_embedding e "
                "JOIN langchain_pg_collection c ON e.collection_id = c.uuid "
                "WHERE c.name = %s",
                (collection,),
            ).fetchone()[0]
    except Exception:
        return 0


def corpus_stats():
    if not os.path.exists(INDEX_STATE):
        return None
    try:
        with open(INDEX_STATE, encoding="utf-8") as fh:
            return json.load(fh)
    except json.JSONDecodeError:
        return None


def text_to_speech(text, lang='en'):
    tts = gTTS(text=text, lang=lang)
    tts.save("output.mp3")
    return "output.mp3"



# Returns history_retriever_chain
# The underscore prefix is required: PGVector wraps a SQLAlchemy engine, which
# Streamlit cannot hash, so it would raise UnhashableParamError on every call.
@st.cache_resource(show_spinner="Loading the reranker...")
def get_retriever_chain(_vector_store):
    llm = groq_chat
    retriever = _vector_store.as_retriever(search_kwargs={'k': 20})
    #compressor = FlashrankRerank(model="ms-marco-MiniLM-L-12-v2")
    rerank_model = HuggingFaceCrossEncoder(model_name="BAAI/bge-reranker-base")
    compressor = CrossEncoderReranker(model=rerank_model, top_n=10)
    compression_retriever = ContextualCompressionRetriever(
        base_compressor=compressor, base_retriever=retriever
    )
    prompt = ChatPromptTemplate.from_messages([
        MessagesPlaceholder(variable_name="chat_history"),
        ("user", "{input}"),
        ("user", """Based on the above conversation, generate a search query that retrieves the most relevant and up-to-date information for the user. Focus on key topics, entities, or concepts that are directly related to the user's query. 
        Make sure the search query is specific and targets the most relevant sources of information.""")
    ])
    history_retriever_chain = create_history_aware_retriever(llm, compression_retriever, prompt)

    return history_retriever_chain

# Returns conversational rag
def get_conversational_rag(history_retriever_chain):
    llm = groq_chat
    answer_prompt = ChatPromptTemplate.from_messages([
        ("system", """
        You are a highly knowledgeable assistant on Bangladeshi land law and administration, your task is to answer any task or query of the user, using information retrieved from the land records corpus: land acts, ordinances, rules, schedules, ebooks, blogs, forum discussions and Q&A.
        Your goal is to provide clear and accurate answers based on the retrieved context. 
        If the answer is not directly available, say: "I couldn't find this information in the provided documents."
        Be concise, but thorough.
        \n\nContext snippets used in response:\n\n{context}"""),
        MessagesPlaceholder(variable_name="chat_history"),
        ("user", "{input}")
    ])

    document_chain = create_stuff_documents_chain(llm, answer_prompt)

    # Create final retrieval chain
    conversational_retrieval_chain = create_retrieval_chain(history_retriever_chain, document_chain)

    return conversational_retrieval_chain

# Returns the final response plus the documents it was grounded in
def get_response(user_input):
    history_retriever_chain = get_retriever_chain(st.session_state.vector_store)
    conversation_rag_chain = get_conversational_rag(history_retriever_chain)
    response = conversation_rag_chain.invoke({
        "chat_history": st.session_state.chat_history,
        "input": user_input
    })
    # create_stuff_documents_chain returns {**kwargs, "answer": ...}, so the
    # retrieved documents arrive under "context" - the chain does NOT produce a
    # "source_documents" key, and .get("source_documents") would silently yield
    # no citations at all.
    docs = response.get("context") or response.get("source_documents") or []
    return response["answer"], docs


SOURCE_LABEL = {
    "act": "Act", "act_text": "Act text", "section": "Section",
    "subsection": "Sub-section", "schedule": "Schedule",
    "subschedule": "Sub-schedule", "qna": "Q&A", "blog": "Blog",
    "forum_group": "Forum", "forum_topic": "Forum topic",
}


def describe(doc):
    """One line naming where a retrieved chunk came from."""
    m = doc.metadata
    kind = SOURCE_LABEL.get(m.get("source_type"), m.get("source_type") or "source")
    if m.get("source_type") == "qna":
        title = m.get("question") or ""
        if m.get("category"):
            title += f" [{m['category']}]"
    elif m.get("act_title"):
        title = m["act_title"]
        if m.get("path") and m["path"] != m["act_title"]:
            title = m["path"]
    else:
        title = m.get("title") or m.get("group_name") or ""
    where = " > ".join(x for x in (m.get("act_year"), title) if x)
    if m.get("act_year") and m.get("number") and m.get("source_type") in (
            "act", "act_text"):
        where = f"{where} (ধারা নং {m['number']})" if m["number"] else where
    return f"{kind} - {where}" if where else kind


def render_sources(docs):
    if not docs:
        return
    with st.expander(f"Sources ({len(docs)})"):
        for i, doc in enumerate(docs, 1):
            m = doc.metadata
            st.markdown(f"**{i}. {describe(doc)}**")
            bits = [f"chunk {(m.get('chunk_index') or 0) + 1}/"
                    f"{m.get('chunk_total') or 1}"]
            if m.get("applicable_date"):
                bits.append(f"applicable {m['applicable_date']}")
            if m.get("signature_by"):
                bits.append(f"signed by {m['signature_by']}")
            st.caption(" | ".join(b for b in bits if b))
            with st.popover("text"):
                st.write(doc.page_content)
            links = []
            if m.get("pdf_url"):
                links.append(f"[PDF]({m['pdf_url']})")
            if m.get("url"):
                links.append(f"[bhumipedia]({m['url']})")
            if links:
                st.markdown(" ".join(links))


# Main app
def main():
    st.set_page_config("AI Assistant")
    st.header("AI Assistant")

    vector_store = get_vector_store()

    # Sidebar
    with st.sidebar:
        st.title("Menu:")
        st.caption(f"Source: {API_BASE}")
        if vector_store is None:
            st.error(
                "No vector store. Check that:\n\n"
                "- `DATABASE_URL` is set in .env\n"
                "- PostgreSQL is running (start Docker Desktop, then "
                "`docker compose up -d`)\n\n"
                "Then build it with `python ingest.py`"
            )
            st.stop()
        stats = corpus_stats() or {}
        st.success(f"Index ready - {stats.get('records', '?')} records")
        st.caption(f"{chunk_count(COLLECTION):,} vectors in pgvector collection '{COLLECTION}'")
        st.divider()
        st.caption("To pick up upstream changes, run `python ingest.py` and reload.")

    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = [AIMessage(content="Hi, how can I help you?")]
    if 'sources' not in st.session_state:
        # Keyed independently of chat_history: a session that started before the
        # citations feature already has chat_history, so a combined guard would
        # skip this and `st.session_state.sources` would never be created.
        # Backfilled to the same length to keep the two lists aligned for zip().
        st.session_state.sources = [None] * len(st.session_state.chat_history)

    st.session_state.vector_store = vector_store

    # User input through chat interface
    user_input = st.chat_input("Type your message here...")
    if user_input is not None and user_input.strip() != "":
        with st.spinner("Thinking..."):
            response, sources = get_response(user_input)

        # Update chat history
        st.session_state.chat_history.append(HumanMessage(content=user_input))
        st.session_state.sources.append(None)
        st.session_state.chat_history.append(AIMessage(content=response))
        st.session_state.sources.append(sources)

    # Display chat history
    for message, sources in zip(st.session_state.chat_history, st.session_state.sources):
        if isinstance(message, AIMessage):
            with st.chat_message("AI"):
                st.write(message.content)
                render_sources(sources)
                try:
                    audio_file = text_to_speech(message.content)
                    st.audio(audio_file, format="audio/mp3")
                except Exception:
                    pass
        else:
            with st.chat_message("Human"):
                st.write(message.content)



if __name__ == "__main__":
    main()
