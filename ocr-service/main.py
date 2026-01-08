#!/usr/bin/env python3

import os
import base64
import logging
import tempfile
from typing import Optional, List, Dict, Any
from io import BytesIO

from fastapi import FastAPI, UploadFile, File, HTTPException, status, Form
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import uvicorn
from mistralai import Mistral

from supabase_client import SupabaseClient

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="OCR Question Extraction Service", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Mistral client
mistral_api_key = os.getenv("MISTRAL_API_KEY")
if not mistral_api_key:
    raise RuntimeError("MISTRAL_API_KEY must be set in the environment")

mistral = Mistral(api_key=mistral_api_key)

# OCR model name - can be overridden via environment variable
OCR_MODEL = os.getenv("MISTRAL_OCR_MODEL", None)  # None uses default model

# Initialize Supabase client
supabase_client = SupabaseClient()

# JSON schema for question extraction
QUESTION_SCHEMA = {
    "type": "object",
    "properties": {
        "questions": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "optionA": {"type": "string"},
                    "optionB": {"type": "string"},
                    "optionC": {"type": "string"},
                    "optionD": {"type": "string"},
                    "optionE": {"type": "string"},
                    "correctAnswer": {"type": "string", "enum": ["A", "B", "C", "D", "E"]},
                    "comment": {"type": "string"}
                },
                "required": ["question", "optionA", "optionB", "optionC", "optionD", "optionE"]
            }
        }
    },
    "required": ["questions"]
}


@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "ok", "service": "ocr-service"}


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "ocr-service"}


def validate_file(file: UploadFile) -> bool:
    """Validate file type and size"""
    allowed_types = {
        "application/pdf": [".pdf"],
        "image/png": [".png"],
        "image/jpeg": [".jpg", ".jpeg"],
        "image/jpg": [".jpg", ".jpeg"]
    }
    
    # Check content type
    if file.content_type not in allowed_types:
        return False
    
    # Check file extension
    filename = file.filename or ""
    ext = os.path.splitext(filename)[1].lower()
    if ext not in allowed_types.get(file.content_type, []):
        return False
    
    return True


async def prepare_document_for_mistral(file: UploadFile) -> Dict[str, Any]:
    """
    Prepare document for Mistral OCR API.
    Returns a document dict that can be used with Mistral OCR.
    """
    content = await file.read()
    file_ext = os.path.splitext(file.filename or "")[1].lower()
    content_type = file.content_type or ""
    
    # For PDFs, we need to upload to a temporary location or use base64
    # Mistral OCR supports document_url, image_url, or file_id
    # For simplicity, we'll use base64 encoding for images and handle PDFs differently
    
    if content_type == "application/pdf":
        # For PDFs, we can use base64 or upload to storage
        # Using base64 with data URI for PDFs
        base64_content = base64.b64encode(content).decode('utf-8')
        # Mistral OCR might need the file uploaded first, but let's try with base64
        # Actually, looking at the API, PDFs might need to be uploaded as files first
        # For now, let's use a temporary approach: save to temp file and provide URL
        # Or use Supabase storage temporarily
        
        # Upload PDF to Supabase storage temporarily to get a URL
        # Mistral OCR requires a publicly accessible URL for PDFs
        try:
            temp_filename = f"ocr_temp_{os.urandom(8).hex()}.pdf"
            bucket = "questions"  # Assuming this bucket exists
            
            public_url = await supabase_client.upload_file_to_storage(
                bucket,
                temp_filename,
                content,
                "application/pdf"
            )
            
            return {
                "document_url": public_url,
                "type": "document_url"
            }
        except Exception as e:
            logger.error(f"Error uploading PDF to storage: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to prepare PDF for OCR: {str(e)}"
            )
    
    elif content_type in ["image/png", "image/jpeg", "image/jpg"]:
        # For images, use base64 encoding
        base64_content = base64.b64encode(content).decode('utf-8')
        mime_type = "image/png" if file_ext == ".png" else "image/jpeg"
        data_uri = f"data:{mime_type};base64,{base64_content}"
        
        return {
            "image_url": {
                "url": data_uri
            },
            "type": "image_url"
        }
    
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {content_type}"
        )


async def extract_questions_from_ocr(ocr_response: Any) -> List[Dict[str, Any]]:
    """
    Extract questions from Mistral OCR response.
    The response should contain document_annotation with JSON string.
    """
    try:
        # Get the document_annotation field which contains the JSON
        document_annotation = ocr_response.document_annotation
        
        if not document_annotation:
            # Fallback: try to extract from pages markdown
            logger.warning("No document_annotation found, trying to parse from pages")
            if hasattr(ocr_response, 'pages') and ocr_response.pages:
                # Extract markdown from first page
                markdown_content = ocr_response.pages[0].markdown if ocr_response.pages else ""
                # This would require additional parsing - for now, return empty
                logger.error("Cannot extract questions from markdown without additional parsing")
                return []
            return []
        
        # Parse JSON string
        import json
        parsed_data = json.loads(document_annotation)
        
        # Extract questions array
        questions = parsed_data.get("questions", [])
        
        if not questions:
            logger.warning("No questions found in OCR response")
            return []
        
        return questions
    
    except Exception as e:
        logger.error(f"Error extracting questions from OCR response: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to parse OCR response: {str(e)}"
        )


def validate_question(question: Dict[str, Any]) -> bool:
    """Validate that a question has all required fields (correctAnswer is optional)"""
    required_fields = ["question", "optionA", "optionB", "optionC", "optionD", "optionE"]
    return all(field in question and question[field] for field in required_fields)


def prepare_question_for_db(
    question: Dict[str, Any],
    user_id: str,
    filename: str,
    visibility: str,
    university_id: Optional[str],
    exam_name: Optional[str],
    exam_year: Optional[str],
    exam_semester: Optional[str],
    subject: Optional[str]
) -> Dict[str, Any]:
    """Convert question from OCR format to database format"""
    correct_answer = question.get("correctAnswer", "").strip().upper()
    # Validate correctAnswer if provided (must be A-E)
    if correct_answer and correct_answer not in ["A", "B", "C", "D", "E"]:
        correct_answer = ""  # Set to empty if invalid
    
    return {
        "user_id": user_id,
        "question": question.get("question", "").strip(),
        "option_a": question.get("optionA", "").strip(),
        "option_b": question.get("optionB", "").strip(),
        "option_c": question.get("optionC", "").strip(),
        "option_d": question.get("optionD", "").strip(),
        "option_e": question.get("optionE", "").strip(),
        "correct_answer": correct_answer,
        "comment": question.get("comment", "").strip() if question.get("comment") else None,
        "subject": subject.strip() if subject else "",
        "filename": filename,
        "difficulty": 3,  # Default difficulty
        "visibility": visibility,
        "university_id": university_id if visibility == "university" else None,
        "exam_name": exam_name.strip() if exam_name else None,
        "exam_year": exam_year.strip() if exam_year else None,
        "exam_semester": exam_semester.strip() if exam_semester else None,
    }


@app.post("/process")
async def process_document(
    file: UploadFile = File(...),
    userId: str = Form(...),
    visibility: str = Form("private"),
    universityId: str = Form(""),
    examName: str = Form(""),
    examYear: str = Form(""),
    examSemester: str = Form(""),
    subject: str = Form("")
):
    """
    Process a document file and extract questions using Mistral OCR.
    """
    try:
        # Validate file
        if not validate_file(file):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid file type. Only PDF, PNG, and JPEG files are supported."
            )
        
        # Validate visibility
        if visibility not in ["private", "university"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Visibility must be 'private' or 'university'"
            )
        
        # Validate university_id if visibility is university
        university_id = universityId.strip() if universityId else None
        if visibility == "university" and not university_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="universityId is required when visibility is 'university'"
            )
        
        logger.info(f"Processing file: {file.filename} for user: {userId}")
        
        # Prepare document for Mistral OCR
        document = await prepare_document_for_mistral(file)
        
        # Call Mistral OCR API
        logger.info("Calling Mistral OCR API...")
        try:
            # Use configured model or None for default
            ocr_response = mistral.ocr.process(
                model=OCR_MODEL,  # None uses default model
                document=document,
                document_annotation_format={
                    "type": "json_schema",
                    "json_schema": QUESTION_SCHEMA
                }
            )
        except Exception as e:
            logger.error(f"Mistral OCR API error: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"OCR processing failed: {str(e)}"
            )
        
        # Extract questions from OCR response
        logger.info("Extracting questions from OCR response...")
        extracted_questions = await extract_questions_from_ocr(ocr_response)
        
        if not extracted_questions:
            return JSONResponse(
                status_code=200,
                content={
                    "success": False,
                    "error": "No questions could be extracted from the document"
                }
            )
        
        # Validate and prepare questions for database
        valid_questions = []
        for q in extracted_questions:
            if validate_question(q):
                db_question = prepare_question_for_db(
                    q,
                    userId,
                    file.filename or "ocr_document",
                    visibility,
                    university_id,
                    examName.strip() if examName else None,
                    examYear.strip() if examYear else None,
                    examSemester.strip() if examSemester else None,
                    subject.strip() if subject else None
                )
                valid_questions.append(db_question)
            else:
                logger.warning(f"Skipping invalid question: {q}")
        
        if not valid_questions:
            return JSONResponse(
                status_code=200,
                content={
                    "success": False,
                    "error": "No valid questions found after validation"
                }
            )
        
        # Insert questions into database
        logger.info(f"Inserting {len(valid_questions)} questions into database...")
        try:
            inserted_questions = await supabase_client.insert_questions(valid_questions)
            logger.info(f"Successfully inserted {len(inserted_questions)} questions")
        except Exception as e:
            logger.error(f"Database insertion error: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to save questions to database: {str(e)}"
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "questions_extracted": len(inserted_questions),
                "message": f"Successfully extracted and saved {len(inserted_questions)} questions"
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}"
        )


if __name__ == "__main__":
    port = int(os.getenv("OCR_SERVICE_PORT", "8002"))
    uvicorn.run(app, host="0.0.0.0", port=port)

