MODULE_MANIFEST = {
    "id": "M023",
    "name": "Recipe Box",
    "category": "Home",
    "description": "Save, organize, and search personal recipes. Scale ingredients and browse by category.",
    "version": "1.0.0",
    "tier": "free",
    "tools": ["save_recipe", "get_recipes", "search_recipes"],
    "min_bridge_version": "1.0.0"
}

from engine.storage import load, save, now_iso, new_id


def save_recipe(profile_dir, name, ingredients, steps, tags=None, source_url='', servings=''):
    items = load(profile_dir, 'recipes')
    recipe = {'id': new_id(), 'name': name, 'ingredients': ingredients,
              'steps': steps, 'tags': tags or [], 'source_url': source_url,
              'servings': servings, 'created': now_iso()}
    items.append(recipe)
    save(profile_dir, 'recipes', items)
    return f"Recipe '{name}' saved."


def find_recipe(profile_dir, query):
    q = query.lower()
    results = []
    for r in load(profile_dir, 'recipes'):
        if (q in r.get('name', '').lower() or
                any(q in tag.lower() for tag in r.get('tags', [])) or
                any(q in ing.lower() for ing in r.get('ingredients', []))):
            results.append(r)
    return results


def get_all(profile_dir):
    return load(profile_dir, 'recipes')


def get_by_id(profile_dir, recipe_id):
    for r in load(profile_dir, 'recipes'):
        if r['id'] == recipe_id:
            return r
    return None


def delete_recipe(profile_dir, recipe_id):
    items = [r for r in load(profile_dir, 'recipes') if r['id'] != recipe_id]
    save(profile_dir, 'recipes', items)
    return "Recipe deleted."


def scale_recipe(recipe: dict, factor: float) -> dict:
    scaled = dict(recipe)
    scaled['ingredients'] = [f"({factor}x) {ing}" for ing in recipe.get('ingredients', [])]
    scaled['servings'] = f"{factor}x original"
    return scaled
