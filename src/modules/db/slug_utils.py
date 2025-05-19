import re

def generate_slug(company_name):
    """
    Generate a URL-friendly slug from a company name
    Similar to the JavaScript function:
    
    export const generateSlug = (companyName) => {
      return _.toLower(companyName)
        .replace(/\s+/g, "-")
        .replace(/[^\w-]/g, "")
        .replace(/(-ltd|-pvt)\.?/g, "")
        .replace(/--+/g, "-");
    };
    """
    if not company_name or not isinstance(company_name, str):
        return ""
        
    # Convert to lowercase
    slug = company_name.lower()
    
    # Replace spaces with hyphens
    slug = re.sub(r'\s+', '-', slug)
    
    # Remove any non-word characters (except hyphens)
    slug = re.sub(r'[^\w-]', '', slug)
    
    # Remove -ltd, -pvt suffixes
    slug = re.sub(r'(-ltd|-pvt)\.?', '', slug)
    
    # Replace multiple consecutive hyphens with a single hyphen
    slug = re.sub(r'--+', '-', slug)
    
    # Remove leading and trailing hyphens
    slug = slug.strip('-')
    
    return slug 